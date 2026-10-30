"""Local MCP server (stdio) in front of the cloud listing agent.

The agent passes photo *file paths*; this shim downscales and uploads them straight to S3, so images never pass through
the model's context.  It holds the WebSocket subscription to /jobs/{jobId} and turns each event into an MCP progress
notification, returning when the job is done, fails, or needs the seller's input.

Environment (values are the CDK stack outputs):
    ECAN_GRAPHQL_URL, ECAN_GRAPHQL_KEY, ECAN_EVENTS_HTTP, ECAN_EVENTS_REALTIME, ECAN_EVENTS_KEY
    ECAN_WAIT_SECONDS   how long one tool call follows the stream before returning "running" (default 600)
"""
import asyncio
import base64
import io
import json
import os
import urllib.request
import uuid

import websockets
from mcp.server.mcpserver import Context, MCPServer

MAX_SIDE = 1600
STOP_TYPES = {"done", "error", "question"}

mcp = MCPServer("ecan-listing", instructions=(
    "Create a marketplace listing from a few product photos and whatever the seller knows. "
    "Call start_listing with local image paths and notes. If it returns status needs_input, ask the seller the "
    "questions and call answer. If it returns running, call get_listing later."))


def _env(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        raise RuntimeError(f"{name} is not set (see the CDK stack outputs)")
    return v


# ---------------------------------------------------------------- GraphQL over HTTPS
JOB_FIELDS = "id status match questions draft images error updatedAt"


def _gql_sync(query: str, variables: dict) -> dict:
    req = urllib.request.Request(_env("ECAN_GRAPHQL_URL"), method="POST",
                                 data=json.dumps({"query": query, "variables": variables}).encode(),
                                 headers={"content-type": "application/json", "x-api-key": _env("ECAN_GRAPHQL_KEY")})
    with urllib.request.urlopen(req, timeout=30) as r:
        out = json.loads(r.read())
    if out.get("errors"):
        raise RuntimeError("; ".join(e.get("message", str(e)) for e in out["errors"]))
    return out["data"]


async def gql(query: str, **variables) -> dict:
    return await asyncio.to_thread(_gql_sync, query, variables)


def _job(j: dict | None) -> dict:
    """AWSJSON fields arrive as JSON strings; decode them for the agent."""
    if not j:
        return {"status": "not_found"}
    return {k: (json.loads(v) if k in ("match", "questions", "draft") and isinstance(v, str) else v) for k, v in j.items()}


# ---------------------------------------------------------------- uploads
def _jpeg(path: str) -> bytes:
    from PIL import Image, ImageOps  # noqa: PLC0415
    with Image.open(path) as im:
        im = ImageOps.exif_transpose(im).convert("RGB")
        im.thumbnail((MAX_SIDE, MAX_SIDE))
        buf = io.BytesIO()
        im.save(buf, "JPEG", quality=88)
        return buf.getvalue()


def _put(url: str, body: bytes) -> None:
    req = urllib.request.Request(url, data=body, method="PUT", headers={"content-type": "image/jpeg"})
    with urllib.request.urlopen(req, timeout=60) as r:
        r.read()


async def upload(job_id: str, paths: list[str]) -> list[str]:
    urls = (await gql("mutation($j: ID!, $n: Int!) { createUploadUrls(jobId: $j, count: $n) { key url } }",
                      j=job_id, n=len(paths)))["createUploadUrls"]
    for p, u in zip(paths, urls):
        await asyncio.to_thread(_put, u["url"], await asyncio.to_thread(_jpeg, p))
    return [u["key"] for u in urls]


# ---------------------------------------------------------------- event stream (AppSync Events WebSocket)
def _b64url(d: dict) -> str:
    return base64.urlsafe_b64encode(json.dumps(d).encode()).decode().rstrip("=")


def _realtime_url() -> str:
    host = _env("ECAN_EVENTS_REALTIME")
    return host if "://" in host else f"wss://{host}/event/realtime"      # a full URL is accepted for local tests


async def latest_seq(job_id: str, after: int = 0) -> int:
    rows = (await gql("query($j: ID!, $a: Int) { events(jobId: $j, afterSeq: $a) { seq event } }",
                      j=job_id, a=after))["events"]
    return max([after] + [r["seq"] for r in rows])


def describe(e: dict) -> str:
    d, t = e.get("data") or {}, e.get("type")
    if t == "stage":
        return f"{e['stage']}: {d.get('lane', '')} {d.get('status', '')}".replace("  ", " ")
    if t == "candidate":
        return f"candidate ({d.get('lane')}): {d.get('title')} [{d.get('confidence')}]"
    if t == "match":
        return f"match: {d.get('level')} {d.get('subcategory') or ''}".strip()
    if t == "draft_delta":
        return f"writing {d.get('field')}: {d.get('text', '')[:80]}"
    if t == "image":
        return f"image ready: {d.get('kind')}"
    if t == "question":
        return "needs input: " + "; ".join(q["text"] for q in d.get("questions", []))
    return f"{e.get('stage')}: {t}"


async def follow(job_id: str, ctx: Context, kick=None, after_seq: int = 0) -> dict:
    """Subscribe to /jobs/{job_id}, run `kick` (the mutation that starts or resumes the job) once subscribed, then
    report events until a stop event or the wait limit.  Reconnects and catches up through the `events` query."""
    http, key = _env("ECAN_EVENTS_HTTP"), _env("ECAN_EVENTS_KEY")
    auth = {"host": http, "x-api-key": key}
    seen, last = set(), after_seq
    deadline = asyncio.get_running_loop().time() + float(os.environ.get("ECAN_WAIT_SECONDS", "600"))

    async def handle(e: dict) -> bool:
        nonlocal last
        if e["seq"] in seen:
            return False
        seen.add(e["seq"])
        last = max(last, e["seq"])
        await ctx.report_progress(len(seen), None, describe(e))
        return e["type"] in STOP_TYPES

    async def catch_up() -> bool:
        rows = (await gql("query($j: ID!, $a: Int) { events(jobId: $j, afterSeq: $a) { seq event } }",
                          j=job_id, a=last))["events"]
        stop = False
        for r in sorted(rows, key=lambda r: r["seq"]):
            stop = await handle(json.loads(r["event"])) or stop
        return stop

    while asyncio.get_running_loop().time() < deadline:
        try:
            async with websockets.connect(_realtime_url(),
                                          subprotocols=["aws-appsync-event-ws", "header-" + _b64url(auth)]) as ws:
                sub_id = str(uuid.uuid4())
                await ws.send(json.dumps({"type": "subscribe", "id": sub_id, "channel": f"/jobs/{job_id}",
                                          "authorization": auth}))
                while True:                                     # wait until the subscription is live
                    m = json.loads(await asyncio.wait_for(ws.recv(), 15))
                    if m.get("type") == "subscribe_success":
                        break
                    if m.get("type") in ("subscribe_error", "error"):
                        raise RuntimeError(f"subscribe failed: {m}")
                if kick:
                    await kick()
                    kick = None
                if await catch_up():                            # anything published before the subscription was live
                    break
                while True:
                    left = deadline - asyncio.get_running_loop().time()
                    if left <= 0:
                        break
                    try:
                        m = json.loads(await asyncio.wait_for(ws.recv(), left))
                    except asyncio.TimeoutError:
                        break
                    if m.get("type") != "data":
                        continue
                    e = json.loads(m["event"])
                    if e["seq"] > last + 1 and await catch_up():   # a publish was lost: fill the gap from the store
                        return await _final(job_id, last)
                    if await handle(e):
                        return await _final(job_id, last)
                break
        except (websockets.ConnectionClosed, OSError):
            await asyncio.sleep(1)                              # reconnect, then catch up
            continue
        break
    return await _final(job_id, last)


async def _final(job_id: str, last: int) -> dict:
    j = _job((await gql(f"query($j: ID!) {{ listing(jobId: $j) {{ {JOB_FIELDS} }} }}", j=job_id))["listing"])
    return {"job_id": job_id, "last_seq": last, **j}


# ---------------------------------------------------------------- tools
@mcp.tool()
async def start_listing(image_paths: list[str], ctx: Context, notes: str = "", marketplace: str = "US",
                        target_price: float | None = None) -> dict:
    """Start a listing job from local product photos (phone pictures are fine) and anything known about the item.
    Streams progress; returns the draft listing, or status needs_input with questions for the seller."""
    if not 1 <= len(image_paths) <= 12:
        raise ValueError("give 1-12 image paths")
    job_id = str(uuid.uuid4())
    await ctx.report_progress(0, None, f"uploading {len(image_paths)} photo(s)")
    keys = await upload(job_id, image_paths)

    async def kick():
        await gql(f"mutation($i: ListingInput!) {{ startListing(input: $i) {{ {JOB_FIELDS} }} }}",
                  i={"jobId": job_id, "imageKeys": keys, "notes": notes, "marketplace": marketplace,
                     "targetPrice": target_price})

    return await follow(job_id, ctx, kick)


@mcp.tool()
async def answer(job_id: str, answers: dict, ctx: Context, last_seq: int = 0) -> dict:
    """Answer the questions of a job in status needs_input (keys = question ids) and follow it to the next stop."""
    last_seq = await latest_seq(job_id, last_seq)        # don't replay the question event that paused the job

    async def kick():
        await gql(f"mutation($j: ID!, $a: AWSJSON!) {{ answer(jobId: $j, answers: $a) {{ {JOB_FIELDS} }} }}",
                  j=job_id, a=json.dumps(answers))

    return await follow(job_id, ctx, kick, after_seq=last_seq)


@mcp.tool()
async def get_listing(job_id: str, ctx: Context, follow_stream: bool = True, last_seq: int = 0) -> dict:
    """Current state of a job.  With follow_stream, waits for the next stop (done, error or question) first."""
    if follow_stream:
        cur = await _final(job_id, last_seq)
        if cur.get("status") in ("queued", "running"):
            return await follow(job_id, ctx, after_seq=last_seq)
        return cur
    return await _final(job_id, last_seq)


def main():
    mcp.run("stdio")


if __name__ == "__main__":
    main()

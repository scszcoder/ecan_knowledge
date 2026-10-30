"""Local MCP server (stdio) in front of the cloud listing agent, which lives on the existing eCan AppSync API.

The agent passes photo *file paths*; this shim downscales and uploads them straight to S3, so images never pass through
the model's context.  It holds the GraphQL subscription onListingJobEvent(jobId) and turns each event into an MCP
progress notification, returning when the job is done, fails, or needs the seller's input.

Environment:
    ECAN_COGNITO_CLIENT_ID   the eCan app client id (same as AuthConfig.COGNITO.CLIENT_ID in eCan.ai)
    ECAN_REFRESH_TOKEN       a Cognito refresh token, or ECAN_USERNAME + ECAN_PASSWORD (USER_PASSWORD_AUTH, as the app),
                             or ECAN_ID_TOKEN (a ready id token; expires after an hour)
    ECAN_GRAPHQL_URL, ECAN_REALTIME_URL   default to the readScreen API
    ECAN_WAIT_SECONDS        how long one tool call follows the stream before returning "running" (default 600)
"""
import asyncio
import base64
import io
import json
import os
import threading
import time
import urllib.parse
import urllib.request
import uuid

import websockets
from mcp.server.mcpserver import Context, MCPServer

REGION = "us-east-1"
GRAPHQL_URL = "https://3oqwpjy5jzal7ezkxrxxmnt6tq.appsync-api.us-east-1.amazonaws.com/graphql"
REALTIME_URL = "wss://3oqwpjy5jzal7ezkxrxxmnt6tq.appsync-realtime-api.us-east-1.amazonaws.com/graphql"
MAX_SIDE = 1600
STOP_TYPES = {"done", "error", "question"}

mcp = MCPServer("ecan-listing", instructions=(
    "Create a marketplace listing from a few product photos and whatever the seller knows. "
    "Call start_listing with local image paths and notes. If it returns status needs_input, ask the seller the "
    "questions and call answer. If it returns running, call get_listing later."))


# ---------------------------------------------------------------- Cognito (same flows as the eCan app)
class Auth:
    def __init__(self):
        self.token, self.expires, self.lock = None, 0.0, threading.Lock()
        self.refresh = os.environ.get("ECAN_REFRESH_TOKEN")

    def id_token(self) -> str:
        with self.lock:
            if self.token and time.time() < self.expires - 300:
                return self.token
            client_id = os.environ.get("ECAN_COGNITO_CLIENT_ID")
            if not client_id or not (self.refresh or os.environ.get("ECAN_USERNAME")):
                static = os.environ.get("ECAN_ID_TOKEN")
                if static:
                    return static
                raise RuntimeError("set ECAN_COGNITO_CLIENT_ID with ECAN_REFRESH_TOKEN or ECAN_USERNAME/ECAN_PASSWORD, "
                                   "or ECAN_ID_TOKEN")
            import boto3                                        # noqa: PLC0415
            from botocore import UNSIGNED                        # noqa: PLC0415
            from botocore.config import Config                   # noqa: PLC0415
            idp = boto3.client("cognito-idp", region_name=REGION, config=Config(signature_version=UNSIGNED))
            if self.refresh:
                r = idp.initiate_auth(ClientId=client_id, AuthFlow="REFRESH_TOKEN_AUTH",
                                      AuthParameters={"REFRESH_TOKEN": self.refresh})["AuthenticationResult"]
            else:
                r = idp.initiate_auth(ClientId=client_id, AuthFlow="USER_PASSWORD_AUTH",
                                      AuthParameters={"USERNAME": os.environ["ECAN_USERNAME"],
                                                      "PASSWORD": os.environ["ECAN_PASSWORD"]})["AuthenticationResult"]
                self.refresh = r.get("RefreshToken")
            self.token, self.expires = r["IdToken"], time.time() + r["ExpiresIn"]
            return self.token


AUTH = Auth()


def _graphql_url() -> str:
    return os.environ.get("ECAN_GRAPHQL_URL", GRAPHQL_URL)


def _auth_header() -> dict:
    return {"host": urllib.parse.urlparse(_graphql_url()).netloc, "Authorization": AUTH.id_token()}


# ---------------------------------------------------------------- GraphQL over HTTPS
JOB_FIELDS = "id status match questions draft images error updatedAt"


def _gql_sync(query: str, variables: dict) -> dict:
    req = urllib.request.Request(_graphql_url(), method="POST",
                                 data=json.dumps({"query": query, "variables": variables}).encode(),
                                 headers={"content-type": "application/json", "Authorization": AUTH.id_token()})
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


EVENTS_Q = "query($j: ID!, $a: Int) { getListingJobEvents(jobId: $j, afterSeq: $a) { seq event } }"


async def latest_seq(job_id: str, after: int = 0) -> int:
    rows = (await gql(EVENTS_Q, j=job_id, a=after))["getListingJobEvents"]
    return max([after] + [r["seq"] for r in rows])


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
    urls = (await gql("mutation($j: ID!, $n: Int!) { createListingUploadUrls(jobId: $j, count: $n) { key url } }",
                      j=job_id, n=len(paths)))["createListingUploadUrls"]
    for p, u in zip(paths, urls):
        await asyncio.to_thread(_put, u["url"], await asyncio.to_thread(_jpeg, p))
    return [u["key"] for u in urls]


# ---------------------------------------------------------------- event stream (AppSync GraphQL subscription)
SUBSCRIPTION = "subscription($j: ID!) { onListingJobEvent(jobId: $j) { jobId seq event } }"


def _b64(d: dict) -> str:
    return base64.b64encode(json.dumps(d).encode()).decode()


def _realtime_url(auth: dict) -> str:
    rt = os.environ.get("ECAN_REALTIME_URL", REALTIME_URL)
    return f"{rt}?header={_b64(auth)}&payload=e30="     # AppSync real-time handshake: auth header + empty payload


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


async def _subscribe(ws, job_id: str, auth: dict) -> None:
    await ws.send(json.dumps({"type": "connection_init"}))
    while json.loads(await asyncio.wait_for(ws.recv(), 15)).get("type") != "connection_ack":
        pass
    await ws.send(json.dumps({"id": str(uuid.uuid4()), "type": "start", "payload": {
        "data": json.dumps({"query": SUBSCRIPTION, "variables": {"j": job_id}}),
        "extensions": {"authorization": auth}}}))
    while True:                                             # wait until the subscription is live
        m = json.loads(await asyncio.wait_for(ws.recv(), 15))
        if m.get("type") == "start_ack":
            return
        if m.get("type") in ("error", "connection_error"):
            raise RuntimeError(f"subscription refused: {m.get('payload')}")


async def follow(job_id: str, ctx: Context, kick=None, after_seq: int = 0) -> dict:
    """Subscribe to onListingJobEvent(job_id), run `kick` (the mutation that starts or resumes the job) once subscribed,
    then report events until a stop event or the wait limit.  Reconnects, and fills any gap in seq (late subscribe,
    reconnect, lost publish) from getListingJobEvents."""
    seen, last = set(), after_seq
    loop = asyncio.get_running_loop()
    deadline = loop.time() + float(os.environ.get("ECAN_WAIT_SECONDS", "600"))

    async def handle(e: dict) -> bool:
        nonlocal last
        if e["seq"] in seen:
            return False
        seen.add(e["seq"])
        last = max(last, e["seq"])
        await ctx.report_progress(len(seen), None, describe(e))
        return e["type"] in STOP_TYPES

    async def catch_up() -> bool:
        rows = (await gql(EVENTS_Q, j=job_id, a=last))["getListingJobEvents"]
        stop = False
        for r in sorted(rows, key=lambda r: r["seq"]):
            stop = await handle(json.loads(r["event"])) or stop
        return stop

    while loop.time() < deadline:
        try:
            auth = await asyncio.to_thread(_auth_header)
            async with websockets.connect(_realtime_url(auth), subprotocols=["graphql-ws"]) as ws:
                await _subscribe(ws, job_id, auth)
                if kick:
                    await kick()
                    kick = None
                if await catch_up():                        # anything published before the subscription was live
                    break
                while (left := deadline - loop.time()) > 0:
                    try:
                        m = json.loads(await asyncio.wait_for(ws.recv(), left))
                    except asyncio.TimeoutError:
                        break
                    if m.get("type") != "data":
                        continue                            # ka (keep-alive) and others
                    row = m["payload"]["data"]["onListingJobEvent"]
                    e = json.loads(row["event"]) if isinstance(row["event"], str) else row["event"]
                    if e["seq"] > last + 1 and await catch_up():   # a publish was lost: fill the gap from the store
                        return await _final(job_id, last)
                    if await handle(e):
                        return await _final(job_id, last)
                break
        except (websockets.ConnectionClosed, OSError):
            await asyncio.sleep(1)                          # reconnect, then catch up
    return await _final(job_id, last)


async def _final(job_id: str, last: int) -> dict:
    j = _job((await gql(f"query($j: ID!) {{ getListingJob(jobId: $j) {{ {JOB_FIELDS} }} }}", j=job_id))["getListingJob"])
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
        await gql(f"mutation($i: ListingJobInput!) {{ startListingJob(input: $i) {{ {JOB_FIELDS} }} }}",
                  i={"jobId": job_id, "imageKeys": keys, "notes": notes, "marketplace": marketplace,
                     "targetPrice": target_price})

    return await follow(job_id, ctx, kick)


@mcp.tool()
async def answer(job_id: str, answers: dict, ctx: Context, last_seq: int = 0) -> dict:
    """Answer the questions of a job in status needs_input (keys = question ids) and follow it to the next stop."""
    last_seq = await latest_seq(job_id, last_seq)        # don't replay the question event that paused the job

    async def kick():
        await gql(f"mutation($j: ID!, $a: AWSJSON!) {{ answerListingJob(jobId: $j, answers: $a) {{ {JOB_FIELDS} }} }}",
                  j=job_id, a=json.dumps(answers))

    return await follow(job_id, ctx, kick, after_seq=last_seq)


@mcp.tool()
async def get_listing(job_id: str, ctx: Context, follow_stream: bool = True, last_seq: int = 0) -> dict:
    """Current state of a job.  With follow_stream, waits for the next stop (done, error or question) first."""
    cur = await _final(job_id, last_seq)
    if follow_stream and cur.get("status") in ("queued", "running"):
        return await follow(job_id, ctx, after_seq=last_seq)
    return cur


def main():
    mcp.run("stdio")


if __name__ == "__main__":
    main()

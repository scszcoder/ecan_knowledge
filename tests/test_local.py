"""End-to-end test without AWS: the real step functions (stubs) and the real MCP shim, with DynamoDB, Step Functions,
GraphQL and the AppSync Events WebSocket replaced by in-memory fakes and a local WebSocket server.

    python -m unittest tests/test_local.py -v
"""
import asyncio
import json
import os
import sys
import threading
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path[:0] = [str(ROOT / "functions"), str(ROOT / "shim")]
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ["STUB_DELAY"] = "0.01"

import websockets  # noqa: E402

import common  # noqa: E402
import steps  # noqa: E402
import ecan_listing_mcp as shim  # noqa: E402


class FakeCloud:
    """Jobs/events store + workflow runner + Events WebSocket server, following the protocol the shim speaks."""

    def __init__(self, drop_first_events: int = 0):
        self.jobs, self.events, self.subs = {}, {}, {}
        self.lock = threading.Lock()
        self.loop = None
        self.drop = drop_first_events          # simulate events published before the client subscribed

    # --- what the step functions call (replaces common.emit / common.set_status)
    def set_status(self, job, status, **fields):
        with self.lock:
            self.jobs.setdefault(job, {"jobId": job})
            self.jobs[job].update(status=status, **fields)

    def emit(self, job, stage, type_, data=None):
        with self.lock:
            evs = self.events.setdefault(job, [])
            evt = {"jobId": job, "seq": len(evs) + 1, "stage": stage, "type": type_, "data": data or {}}
            evs.append(evt)
        if evt["seq"] > self.drop:
            for ws in list(self.subs.get(job, [])):
                asyncio.run_coroutine_threadsafe(
                    ws.send(json.dumps({"type": "data", "id": "x", "event": json.dumps(evt)})), self.loop)
        return evt

    # --- the state machine, run in a thread
    def run(self, state):
        state = steps.intake(state, None)
        state["lanes"] = [steps.lane_catalog(state, None), steps.lane_web_image(state, None), steps.lane_our_db(state, None)]
        state = steps.verify(state, None)
        if state["questions"]:
            self.paused = state
            steps.ask_seller({"taskToken": "tok", "state": state}, None)
            return
        self.compose(state)

    def compose(self, state):
        for f in (steps.draft, steps.validate, steps.images, steps.finish):
            state = f(state, None)

    # --- GraphQL (replaces shim._gql_sync)
    def gql(self, query, v):
        if "createUploadUrls" in query:
            return {"createUploadUrls": [{"key": f"uploads/{v['j']}/{i}.jpg", "url": "fake"} for i in range(v["n"])]}
        if "startListing" in query:
            i = v["i"]
            self.set_status(i["jobId"], "queued")
            threading.Thread(target=self.run, args=({"jobId": i["jobId"], "input": i},)).start()
            return {"startListing": {"id": i["jobId"], "status": "queued"}}
        if "answer(" in query:
            st = {**self.paused, "answers": json.loads(v["a"])}
            self.set_status(v["j"], "running", taskToken=None)
            threading.Thread(target=self.compose, args=(st,)).start()
            return {"answer": {"id": v["j"], "status": "running"}}
        if "events(" in query:
            with self.lock:
                return {"events": [{"seq": e["seq"], "event": json.dumps(e)}
                                   for e in self.events.get(v["j"], []) if e["seq"] > (v["a"] or 0)]}
        if "listing(" in query:
            j = self.jobs.get(v["j"], {})
            return {"listing": {"id": v["j"], "status": j.get("status"),
                                "questions": json.dumps(j["questions"]) if j.get("questions") else None,
                                "draft": json.dumps(j["draft"]) if j.get("draft") else None,
                                "images": j.get("images") or []}}
        raise AssertionError(query)

    async def ws_handler(self, ws):
        msg = json.loads(await ws.recv())
        assert msg["type"] == "subscribe" and msg["authorization"]["x-api-key"] == "k"
        job = msg["channel"].split("/")[-1]
        self.subs.setdefault(job, []).append(ws)
        await ws.send(json.dumps({"type": "subscribe_success", "id": msg["id"]}))
        await ws.wait_closed()


class FakeCtx:
    def __init__(self):
        self.messages = []

    async def report_progress(self, progress, total=None, message=None):
        self.messages.append(message)


class EndToEnd(unittest.TestCase):
    def go(self, cloud: FakeCloud):
        async def scenario():
            cloud.loop = asyncio.get_running_loop()
            async with websockets.serve(cloud.ws_handler, "127.0.0.1", 0, subprotocols=["aws-appsync-event-ws"]) as srv:
                port = srv.sockets[0].getsockname()[1]
                os.environ.update(ECAN_GRAPHQL_URL="x", ECAN_GRAPHQL_KEY="k", ECAN_EVENTS_HTTP="h", ECAN_EVENTS_KEY="k",
                                  ECAN_EVENTS_REALTIME=f"ws://127.0.0.1:{port}", ECAN_WAIT_SECONDS="20")
                shim._gql_sync = cloud.gql
                shim._jpeg = lambda p: b"jpeg"
                shim._put = lambda u, b: None
                c1, c2 = FakeCtx(), FakeCtx()
                r1 = await shim.start_listing(["a.jpg", "b.jpg"], c1, notes="green garden hose reel")
                r2 = await shim.answer(r1["job_id"], {"target_price": 39.99}, c2)
                return r1, r2, c1, c2

        common.emit, common.set_status = cloud.emit, cloud.set_status
        return asyncio.run(scenario())

    def check(self, cloud):
        r1, r2, c1, c2 = self.go(cloud)
        self.assertEqual(r1["status"], "needs_input")
        self.assertEqual(r1["questions"][0]["id"], "target_price")
        self.assertTrue(any(m.startswith("match:") for m in c1.messages))
        self.assertEqual(r2["status"], "done")
        self.assertEqual(r2["draft"]["price"], 39.99)
        self.assertTrue(any("writing title" in m for m in c2.messages))
        self.assertFalse(any(m.startswith("needs input") for m in c2.messages), "question replayed after answer")
        seqs = [e["seq"] for e in cloud.events[r1["job_id"]]]
        self.assertEqual(len(c1.messages) + len(c2.messages), len(seqs) + 1, "each event reported exactly once")
        return r1

    def test_stream(self):
        self.check(FakeCloud())

    def test_missed_events_recovered_by_catch_up(self):
        self.check(FakeCloud(drop_first_events=3))


if __name__ == "__main__":
    unittest.main()

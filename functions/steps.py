"""Workflow steps (one Lambda each, handler `steps.<name>`).  STUBS: they emit realistic events with placeholder data so
the API, the stream and the MCP shim can be built and tested end to end before the real identification logic exists.

State passed between steps:  {jobId, input{imageKeys, notes, marketplace, targetPrice}, facts, lanes[], match, questions,
answers, draft, images}.  Lane steps return only their own result; the Parallel state collects them into `lanes`.
"""
import os
import time

import common

DELAY = float(os.environ.get("STUB_DELAY", "0.3"))     # simulated work between events


def _pause(n: float = 1.0) -> None:
    if DELAY:
        time.sleep(DELAY * n)


def intake(state, _ctx):
    job, inp = state["jobId"], state["input"]
    common.set_status(job, "running")
    common.emit(job, "intake", "stage", {"status": "started", "images": len(inp["imageKeys"])})
    for i, key in enumerate(inp["imageKeys"]):
        _pause()
        common.emit(job, "intake", "found", {"image": i, "key": key, "barcode": None, "ocr": [], "note": "stub"})
    facts = {"notes": inp["notes"], "type_guess": "stub product", "attributes": {}}
    common.emit(job, "intake", "stage", {"status": "done", "facts": facts})
    return {**state, "facts": facts}


def _lane(name: str, state: dict, candidates: list[dict]) -> dict:
    job = state["jobId"]
    common.emit(job, "identify", "stage", {"lane": name, "status": "started"})
    _pause(2)
    for c in candidates:
        common.emit(job, "identify", "candidate", {"lane": name, **c})
    return {"lane": name, "candidates": candidates}


def lane_catalog(state, _ctx):
    return _lane("catalog", state, [])                        # barcode / model number → Amazon catalog (SP-API)


def lane_web_image(state, _ctx):
    return _lane("web_image", state, [{"title": "stub: similar item on the web", "url": None, "confidence": 0.6}])


def lane_our_db(state, _ctx):
    return _lane("our_db", state, [{"title": "stub: closest scraped winner", "asin": None, "subcategory": "stub",
                                    "confidence": 0.7}])


def verify(state, _ctx):
    job = state["jobId"]
    cands = sorted((c for lane in state["lanes"] for c in lane["candidates"]), key=lambda c: -c.get("confidence", 0))
    _pause()
    top = cands[0] if cands else None
    match = {"level": "subcategory" if top else "unknown", "subcategory": (top or {}).get("subcategory"),
             "candidate": top}
    common.emit(job, "identify", "match", match)
    questions = []
    if state["input"].get("targetPrice") is None:
        questions.append({"id": "target_price", "text": "What price do you want to sell at (USD)?", "type": "number"})
    return {**state, "match": match, "questions": questions}


def ask_seller(event, _ctx):
    """Invoked with a task token; the workflow waits until the `answer` mutation returns the token."""
    state = event["state"]
    job = state["jobId"]
    common.set_status(job, "needs_input", taskToken=event["taskToken"], questions=state["questions"])
    common.emit(job, "input", "question", {"questions": state["questions"]})
    return {}


def draft(state, _ctx):
    job = state["jobId"]
    common.emit(job, "draft", "stage", {"status": "started"})
    title = "Stub Brand Product Name, Key Feature, Size, Material, for Use Case"
    words = title.split()
    for i in range(0, len(words), 3):                          # streamed in small chunks, as the real LLM step will
        _pause(0.5)
        common.emit(job, "draft", "draft_delta", {"field": "title", "text": " ".join(words[i:i + 3]) + " "})
    bullets = [f"STUB BULLET {n}: benefit, spec, and the buyer complaint it answers" for n in range(1, 6)]
    for n, b in enumerate(bullets):
        _pause()
        common.emit(job, "draft", "draft_delta", {"field": f"bullet_{n + 1}", "text": b})
    price = (state.get("answers") or {}).get("target_price") or state["input"].get("targetPrice")
    out = {"title": title, "bullets": bullets, "description": "stub", "search_terms": [], "price": price}
    return {**state, "draft": out}


def validate(state, _ctx):
    job = state["jobId"]
    common.emit(job, "validate", "stage", {"status": "started"})
    _pause()
    common.emit(job, "validate", "stage", {"status": "done", "issues": []})   # SP-API VALIDATION_PREVIEW goes here
    return state


def images(state, _ctx):
    job = state["jobId"]
    keys = []
    for kind in ("main_white_bg", "lifestyle", "infographic"):
        _pause()
        key = f"outputs/{job}/{kind}.jpg"
        keys.append(key)
        common.emit(job, "images", "image", {"kind": kind, "key": key, "stub": True})
    return {**state, "images": keys}


def finish(state, _ctx):
    job = state["jobId"]
    common.set_status(job, "done", match=state.get("match"), draft=state.get("draft"), images=state.get("images") or [])
    common.emit(job, "done", "done", {"draft": state.get("draft"), "images": state.get("images") or []})
    return {"jobId": job, "status": "done"}


def fail(state, _ctx):
    job = state["jobId"]
    err = state.get("error") or {}
    msg = f"{err.get('Error', 'Error')}: {err.get('Cause', '')}"[:1000]
    common.set_status(job, "failed", error=msg)
    common.emit(job, "done", "error", {"error": msg})
    return {"jobId": job, "status": "failed"}

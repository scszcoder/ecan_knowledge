"""Shared by the API resolver and the workflow steps: job state in DynamoDB, and the event stream.

Every event is written to the Events table first (jobId, seq) and then published through the existing AppSync API's
IAM-only mutation publishListingJobEvent, which feeds the subscription onListingJobEvent(jobId).  A client that
subscribes late or reconnects fetches what it missed with getListingJobEvents.
"""
import json
import logging
import os
import time
import urllib.request

import boto3
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest

log = logging.getLogger()
log.setLevel(logging.INFO)

_ddb = boto3.resource("dynamodb")
JOBS = _ddb.Table(os.environ.get("JOBS_TABLE", "jobs"))
EVENTS = _ddb.Table(os.environ.get("EVENTS_TABLE", "events"))
EVENT_TTL_DAYS = 30


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def set_status(job_id: str, status: str, **fields) -> None:
    fields = {"status": status, "updatedAt": now_iso(), **fields}
    names = {f"#{k}": k for k in fields}
    values = {f":{k}": v for k, v in fields.items()}
    JOBS.update_item(Key={"jobId": job_id},
                     UpdateExpression="SET " + ", ".join(f"#{k} = :{k}" for k in fields),
                     ExpressionAttributeNames=names, ExpressionAttributeValues=values)


def emit(job_id: str, stage: str, type_: str, data: dict | None = None) -> dict:
    seq = int(JOBS.update_item(Key={"jobId": job_id}, UpdateExpression="ADD seq :one",
                               ExpressionAttributeValues={":one": 1}, ReturnValues="UPDATED_NEW")["Attributes"]["seq"])
    evt = {"jobId": job_id, "seq": seq, "ts": now_iso(), "stage": stage, "type": type_, "data": data or {}}
    # stored as a JSON string: no Decimal conversion, and it is what the API returns as AWSJSON
    EVENTS.put_item(Item={"jobId": job_id, "seq": seq, "event": json.dumps(evt, ensure_ascii=False),
                          "expiresAt": int(time.time()) + EVENT_TTL_DAYS * 86400})
    try:
        publish(job_id, seq, evt)
    except Exception:                       # the event is persisted; subscribers recover it through the catch-up query
        log.exception("publish failed for %s seq %s", job_id, seq)
    return evt


PUBLISH = "mutation($i: ListingJobEventInput!) { publishListingJobEvent(input: $i) { jobId seq event } }"


def publish(job_id: str, seq: int, evt: dict) -> None:
    """Run the publish mutation on the AppSync API, signed with the function's IAM role."""
    url = os.environ["APPSYNC_GRAPHQL_URL"]
    body = json.dumps({"query": PUBLISH, "variables": {"i": {"jobId": job_id, "seq": seq,
                                                              "event": json.dumps(evt, ensure_ascii=False)}}}).encode()
    req = AWSRequest(method="POST", url=url, data=body, headers={"content-type": "application/json"})
    SigV4Auth(boto3.Session().get_credentials(), "appsync", os.environ["AWS_REGION"]).add_auth(req)
    with urllib.request.urlopen(urllib.request.Request(url, data=body, headers=dict(req.headers), method="POST"),
                                timeout=5) as r:
        out = json.loads(r.read())
    if out.get("errors"):
        raise RuntimeError(f"publishListingJobEvent: {out['errors']}")

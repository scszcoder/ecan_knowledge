"""Direct Lambda resolver for the ecan_knowledge fields on the existing AppSync API (see appsync/additions.graphql).

Callers are Cognito users; every job belongs to the user who reserved it with createListingUploadUrls, and only that
user can start it, read it, answer it or subscribe to its events.
"""
import json
import os
import re

import boto3

import common

s3 = boto3.client("s3")
sfn = boto3.client("stepfunctions")
BUCKET = os.environ.get("UPLOAD_BUCKET", "")
STATE_MACHINE = os.environ.get("STATE_MACHINE_ARN", "")
JOB_ID = re.compile(r"^[A-Za-z0-9-]{8,80}$")     # also used as the Step Functions execution name
MAX_IMAGES = 12


class Forbidden(Exception):
    pass


def handler(event, _ctx):
    field = event["info"]["fieldName"]
    owner = (event.get("identity") or {}).get("sub")
    if not owner:
        raise Forbidden("sign in required")
    return FIELDS[field](owner, **(event.get("arguments") or {}))


def _job_id(job_id: str) -> str:
    if not JOB_ID.match(job_id or ""):
        raise ValueError("jobId must be 8-80 characters of letters, digits and '-' (use a UUID)")
    return job_id


def _own(owner: str, job_id: str) -> dict:
    item = common.JOBS.get_item(Key={"jobId": _job_id(job_id)}).get("Item")
    if not item or item.get("owner") != owner:
        raise Forbidden("no such job")              # same answer for missing and foreign jobs
    return item


def _shape(item: dict) -> dict:
    js = lambda k: json.dumps(item[k], ensure_ascii=False, default=str) if item.get(k) is not None else None  # noqa: E731
    return {"id": item["jobId"], "status": item.get("status", "queued"), "match": js("match"),
            "questions": js("questions"), "draft": js("draft"), "images": item.get("images") or [],
            "error": item.get("error"), "updatedAt": item.get("updatedAt")}


def create_upload_urls(owner, jobId, count):
    """Reserves the jobId for this user (status `uploading`) and returns pre-signed PUT links for its photos."""
    jobId = _job_id(jobId)
    if not 1 <= count <= MAX_IMAGES:
        raise ValueError(f"count must be 1-{MAX_IMAGES}")
    try:
        common.JOBS.put_item(Item={"jobId": jobId, "owner": owner, "status": "uploading", "seq": 0,
                                   "updatedAt": common.now_iso()},
                             ConditionExpression="attribute_not_exists(jobId) OR (#o = :o AND #s = :up)",
                             ExpressionAttributeNames={"#o": "owner", "#s": "status"},
                             ExpressionAttributeValues={":o": owner, ":up": "uploading"})
    except common.JOBS.meta.client.exceptions.ConditionalCheckFailedException:
        raise Forbidden("jobId already in use") from None
    out = []
    for i in range(count):
        key = f"uploads/{jobId}/{i}.jpg"
        url = s3.generate_presigned_url("put_object", Params={"Bucket": BUCKET, "Key": key, "ContentType": "image/jpeg"},
                                        ExpiresIn=900)
        out.append({"key": key, "url": url})
    return out


def start_listing(owner, input):
    job_id = _job_id(input["jobId"])
    item = _own(owner, job_id)
    if item.get("status") != "uploading":
        return _shape(item)                     # retried call: already started
    keys = input.get("imageKeys") or []
    if not keys or any(not k.startswith(f"uploads/{job_id}/") for k in keys):
        raise ValueError("imageKeys must come from createListingUploadUrls for this jobId")
    job_input = {"imageKeys": keys, "notes": input.get("notes") or "", "marketplace": input.get("marketplace") or "US",
                 "targetPrice": input.get("targetPrice")}
    try:
        common.JOBS.update_item(Key={"jobId": job_id}, ConditionExpression="#s = :up",
                                UpdateExpression="SET #s = :q, #i = :i, updatedAt = :t",
                                ExpressionAttributeNames={"#s": "status", "#i": "input"},
                                ExpressionAttributeValues={":up": "uploading", ":q": "queued",
                                                           ":i": json.dumps(job_input), ":t": common.now_iso()})
    except common.JOBS.meta.client.exceptions.ConditionalCheckFailedException:
        return _shape(_own(owner, job_id))      # lost a race with a concurrent retry
    sfn.start_execution(stateMachineArn=STATE_MACHINE, name=job_id,
                        input=json.dumps({"jobId": job_id, "input": job_input}))
    return _shape(_own(owner, job_id))


def answer(owner, jobId, answers):
    item = _own(owner, jobId)
    if item.get("status") != "needs_input" or not item.get("taskToken"):
        raise ValueError("job is not waiting for input")
    if isinstance(answers, str):
        answers = json.loads(answers)
    common.JOBS.update_item(Key={"jobId": jobId}, UpdateExpression="REMOVE taskToken")
    common.set_status(jobId, "running")
    sfn.send_task_success(taskToken=item["taskToken"], output=json.dumps(answers, ensure_ascii=False))
    return _shape(_own(owner, jobId))


def listing(owner, jobId):
    return _shape(_own(owner, jobId))


def events(owner, jobId, afterSeq=None):
    from boto3.dynamodb.conditions import Key   # noqa: PLC0415
    _own(owner, jobId)
    cond = Key("jobId").eq(jobId) & Key("seq").gt(afterSeq or 0)
    items, kw = [], {}
    while True:
        r = common.EVENTS.query(KeyConditionExpression=cond, **kw)
        items += r["Items"]
        if "LastEvaluatedKey" not in r:
            break
        kw["ExclusiveStartKey"] = r["LastEvaluatedKey"]
    return [{"jobId": jobId, "seq": int(i["seq"]), "event": i["event"]} for i in items]


def subscribe(owner, jobId):
    """Subscription resolver: runs when a client subscribes; raising refuses the subscription."""
    _own(owner, jobId)
    return None


FIELDS = {"createListingUploadUrls": create_upload_urls, "startListingJob": start_listing,
          "answerListingJob": answer, "getListingJob": listing, "getListingJobEvents": events,
          "onListingJobEvent": subscribe}

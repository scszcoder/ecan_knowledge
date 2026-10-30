"""AppSync Lambda (direct) resolver for every Query/Mutation field in api/schema.graphql."""
import json
import os
import re

import boto3

import common

s3 = boto3.client("s3")
sfn = boto3.client("stepfunctions")
BUCKET = os.environ.get("UPLOAD_BUCKET", "")
STATE_MACHINE = os.environ.get("STATE_MACHINE_ARN", "")
JOB_ID = re.compile(r"^[A-Za-z0-9-]{8,50}$")     # also a valid Events channel segment and execution name
MAX_IMAGES = 12


def handler(event, _ctx):
    field = event["info"]["fieldName"]
    args = event.get("arguments") or {}
    return FIELDS[field](**args)


def _job_id(job_id: str) -> str:
    if not JOB_ID.match(job_id or ""):
        raise ValueError("jobId must be 8-50 characters of letters, digits and '-' (use a UUID)")
    return job_id


def _shape(item: dict | None) -> dict | None:
    if not item:
        return None
    js = lambda k: json.dumps(item[k], ensure_ascii=False, default=str) if item.get(k) is not None else None  # noqa: E731
    return {"id": item["jobId"], "status": item.get("status", "queued"), "match": js("match"),
            "questions": js("questions"), "draft": js("draft"), "images": item.get("images") or [],
            "error": item.get("error"), "updatedAt": item.get("updatedAt")}


def create_upload_urls(jobId, count):
    jobId = _job_id(jobId)
    if not 1 <= count <= MAX_IMAGES:
        raise ValueError(f"count must be 1-{MAX_IMAGES}")
    out = []
    for i in range(count):
        key = f"uploads/{jobId}/{i}.jpg"
        url = s3.generate_presigned_url("put_object", Params={"Bucket": BUCKET, "Key": key, "ContentType": "image/jpeg"},
                                        ExpiresIn=900)
        out.append({"key": key, "url": url})
    return out


def start_listing(input):
    job_id = _job_id(input["jobId"])
    keys = input.get("imageKeys") or []
    if any(not k.startswith(f"uploads/{job_id}/") for k in keys):
        raise ValueError("imageKeys must come from createUploadUrls for this jobId")
    job_input = {"imageKeys": keys, "notes": input.get("notes") or "", "marketplace": input.get("marketplace") or "US",
                 "targetPrice": input.get("targetPrice")}
    try:
        common.JOBS.put_item(Item={"jobId": job_id, "status": "queued", "seq": 0, "updatedAt": common.now_iso(),
                                   "input": json.dumps(job_input)},
                             ConditionExpression="attribute_not_exists(jobId)")
    except common.JOBS.meta.client.exceptions.ConditionalCheckFailedException:
        return listing(job_id)              # retried call: the job already exists
    sfn.start_execution(stateMachineArn=STATE_MACHINE, name=job_id,
                        input=json.dumps({"jobId": job_id, "input": job_input}))
    return listing(job_id)


def answer(jobId, answers):
    item = common.JOBS.get_item(Key={"jobId": _job_id(jobId)}).get("Item")
    if not item or item.get("status") != "needs_input" or not item.get("taskToken"):
        raise ValueError("job is not waiting for input")
    if isinstance(answers, str):
        answers = json.loads(answers)
    common.JOBS.update_item(Key={"jobId": jobId}, UpdateExpression="REMOVE taskToken")
    common.set_status(jobId, "running")
    sfn.send_task_success(taskToken=item["taskToken"], output=json.dumps(answers, ensure_ascii=False))
    return listing(jobId)


def listing(jobId):
    return _shape(common.JOBS.get_item(Key={"jobId": _job_id(jobId)}).get("Item"))


def events(jobId, afterSeq=None):
    from boto3.dynamodb.conditions import Key   # noqa: PLC0415
    cond = Key("jobId").eq(_job_id(jobId)) & Key("seq").gt(afterSeq or 0)
    items, kw = [], {}
    while True:
        r = common.EVENTS.query(KeyConditionExpression=cond, **kw)
        items += r["Items"]
        if "LastEvaluatedKey" not in r:
            break
        kw["ExclusiveStartKey"] = r["LastEvaluatedKey"]
    return [{"seq": int(i["seq"]), "event": i["event"]} for i in items]


FIELDS = {"createUploadUrls": create_upload_urls, "startListing": start_listing, "answer": answer,
          "listing": listing, "events": events}

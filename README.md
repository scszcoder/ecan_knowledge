# ecan_knowledge — cloud listing agent

A serverless AWS service that turns vague product input (a few phone photos, some notes) into a marketplace listing.
It lives **on the existing eCan AppSync API** (`readScreen`, Cognito auth): this repo only *adds* types, fields, data
sources and resolvers there, never changes or removes anything.  Local agents reach it through an MCP tool and get
**streamed progress** while the job runs.

```
local agent ──MCP stdio──> shim/ecan_listing_mcp.py ──GraphQL (Cognito)──> readScreen ──> api λ ──> Step Functions
                              │  photos: pre-signed PUT ──> S3                                        │ step λs
                              └──<── subscription onListingJobEvent ── readScreen <── publishListingJobEvent (IAM)
```

Status: **scaffold**.  API, workflow, streaming and the MCP shim work end to end; the workflow steps are stubs that
emit realistic events with placeholder data.  See [docs/DESIGN.md](docs/DESIGN.md) for the design and the build order.

## Layout

| path | what |
|---|---|
| `appsync/additions.graphql` | the types and fields this repo adds to `readScreen` (additions only) |
| `tools/appsync_add.py` | merges the additions into the **live** schema and attaches our data sources and resolvers; dry run by default |
| `functions/api.py` | direct Lambda resolver for the new fields; jobs belong to the Cognito user who created them |
| `functions/steps.py` | workflow steps (stubs): intake → 3 identify lanes → verify → ask seller? → draft → validate → images |
| `functions/common.py` | job state and `emit()`: write the event to DynamoDB, then publish it through `publishListingJobEvent` |
| `infra/stack.py` | CDK stack: S3, DynamoDB, Step Functions, Lambdas, IAM.  Creates nothing in AppSync |
| `shim/ecan_listing_mcp.py` | local MCP server: Cognito login, uploads photos, follows the subscription, reports MCP progress |
| `tests/test_local.py` | end-to-end test without AWS (fake cloud + local AppSync real-time server) |

## What is added to `readScreen`

| kind | names |
|---|---|
| types / inputs | `ListingJob`, `ListingUploadUrl`, `ListingJobEvent`, `ListingJobInput`, `ListingJobEventInput` |
| Query | `getListingJob`, `getListingJobEvents` |
| Mutation | `createListingUploadUrls`, `startListingJob`, `answerListingJob`, `publishListingJobEvent` (IAM only) |
| Subscription | `onListingJobEvent(jobId)` |
| data sources | `ecanKnowledgeApi` (Lambda), `ecanKnowledgeEvents` (NONE, for the publish mutation) |

The same block is in `eCan_lambda/us/aws/agentScheduler/graphql/agent.graphql`, so that repo's `appsync_apply.py` still
sees every live field.  Keep the two in sync when the additions change.

## Setup and test

```powershell
python -m venv .venv; .venv\Scripts\activate
pip install -r requirements.txt
python -m unittest tests/test_local.py          # no AWS needed
```

## Deploy (profile `sc`, us-east-1)

`sc` cannot bootstrap CDK or create roles; an admin must do a one-time step first, see [deploy/README.md](deploy/README.md).

Note: this machine has an `AWS_ACCESS_KEY_ID` environment variable with an invalid key; pass `--profile sc` explicitly.

```powershell
npx aws-cdk@2 bootstrap aws://667118410653/us-east-1 --profile sc     # once per account/region (not done yet)
npx aws-cdk@2 deploy --profile sc                                     # Lambdas, workflow, tables, bucket, IAM
$env:AWS_PROFILE="sc"; python tools/appsync_add.py                    # dry run: lists exactly what will be added
python tools/appsync_add.py --apply                                   # push schema additions, data sources, resolvers
```

Order matters: the stack first (the resolvers point at its Lambda), then the schema.  `appsync_add.py` re-reads the live
schema after the push and stops if any previously live field is missing.  Tables and the bucket are `RETAIN`ed.

## Use from a local agent

```powershell
claude mcp add ecan-listing `
  -e ECAN_COGNITO_CLIENT_ID=<eCan app client id> -e ECAN_USERNAME=<user> -e ECAN_PASSWORD=<password> `
  -- C:\path\to\ecan_knowledge\.venv\Scripts\python.exe C:\path\to\ecan_knowledge\shim\ecan_listing_mcp.py
```

(`ECAN_REFRESH_TOKEN` instead of user/password, or `ECAN_ID_TOKEN` for a quick test.)

Tools:

- `start_listing(image_paths, notes?, marketplace?, target_price?)` — uploads the photos, starts the job, streams
  progress, and returns the draft, or `status: needs_input` with questions.
- `answer(job_id, answers)` — answers the questions and follows the job to the next stop.
- `get_listing(job_id)` — current state; waits for the next stop if the job is still running.

A tool call follows the stream for up to `ECAN_WAIT_SECONDS` (default 600) and then returns `status: running`.

## Event stream

Every step emits an event, delivered by `onListingJobEvent(jobId)`; `event` is JSON:

```json
{"jobId": "…", "seq": 14, "ts": "…", "stage": "identify", "type": "candidate", "data": {"lane": "web_image", "…": "…"}}
```

Types: `stage`, `found`, `candidate`, `match`, `question`, `draft_delta`, `image`, `done`, `error`.
Events are written to DynamoDB before they are published.  A client generates the jobId, subscribes, then starts the
job, and fills any gap in `seq` (late subscribe, reconnect, lost publish) with `getListingJobEvents`.  Only the job's
owner can subscribe (the subscription resolver checks it).

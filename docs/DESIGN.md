# Design

## Goal

Given vague, incomplete input (a few phone photos, maybe a sentence), produce a marketplace listing: title, bullets,
description, search terms, price, and images in the style of the subcategory's best sellers.  The work is mostly
**identification**: once we know what the item is, the listing follows from the subcategory playbook.

## Pipeline

1. **Intake** (cheap, seconds): barcode / UPC / EAN decode, OCR of labels and packaging (brand, model, ratings),
   background removal and crop, vision-model attribute extraction (type, material, colour, count, size).
2. **Identify**, three lanes in parallel:
   - *catalog*: barcode or model number → Amazon catalog (SP-API `searchCatalogItems` by identifier) and the web;
   - *web image*: Google Cloud Vision Web Detection and/or Google Lens (SerpAPI); 1688 / Alibaba image search for
     sourced goods (check each provider's pricing and terms);
   - *our DB*: image embeddings (SigLIP/CLIP or Bedrock Titan Multimodal) of the scraped products, queried with the
     **cut-out**, not the raw phone photo, plus title text embeddings.
3. **Verify**: a vision model compares the seller's photos with the top candidates →
   `exact` (already on Amazon: offer on the existing ASIN, do **not** create a duplicate listing) | `variant` |
   `subcategory` | `unknown`.
4. **Ask only what is missing**: required attributes for the product type (SP-API Product Type Definitions) minus what
   is already known from notes, OCR, barcode or the matched product.  One short round of questions at most.
5. **Draft**: subcategory playbook + top winners + their review complaints → title, bullets, description, search terms,
   price within the winners' band.  Never copy competitor text or images.
6. **Validate**: SP-API `putListingsItem` in `VALIDATION_PREVIEW` mode, length limits, banned claims, similarity check
   against the matched listings.
7. **Images**: from the seller's own photos (white-background main image, lifestyle scenes, infographics with the
   seller's specs), never a product invented from text.

Every attribute carries provenance (`seller | ocr | barcode | matched:<ASIN> | inferred`) and a confidence.

## AWS shape

Everything sits behind the **existing** AppSync API `readScreen` (Cognito user pools; API key, Lambda and IAM as
additional modes), which already streams other features with GraphQL subscriptions fed by publish-only mutations on
NONE data sources.  This repo follows the same pattern and only adds to the API (see `appsync/additions.graphql`).

| need | service |
|---|---|
| requests | new fields on `readScreen`, direct Lambda resolver (`functions/api.py`), data source `ecanKnowledgeApi` |
| stream | `publishListingJobEvent` (IAM only, NONE data source `ecanKnowledgeEvents`) → `onListingJobEvent(jobId)` |
| ownership | job reserved for the Cognito `sub` by `createListingUploadUrls`; every field and the subscription check it |
| job | Step Functions Standard; `ask_seller` uses `.waitForTaskToken`, resumed by `answerListingJob` |
| state | DynamoDB `Jobs` (owner, status, draft, task token, seq counter) and `Events` (jobId, seq; TTL 30 days) |
| photos | S3, pre-signed PUT from the shim |
| product DB + vectors (next) | Aurora Serverless v2 + pgvector, or S3 Vectors for kNN only |

Schema changes go through `tools/appsync_add.py`: base = live schema (introspection), additions merged in, refuse on any
removed/changed live field or conflicting name, verify after the push.  The CDK stack creates nothing in AppSync.

AppSync resolvers time out at ~30 s, so `startListingJob` only starts the workflow; all progress goes through the
subscription.  Subscription messages are size-limited: events carry status and small text deltas, large outputs go to
S3 by key.  LLM text is published in chunks (~300 ms or per field), not per token.

## Streaming contract

1. Client generates the jobId, reserves it with `createListingUploadUrls`, subscribes to `onListingJobEvent(jobId)`,
   waits for `start_ack`, then calls `startListingJob`.
2. Each event has a per-job `seq`; it is written to DynamoDB before it is published.
3. After subscribing, and whenever a received `seq` skips ahead, the client calls `getListingJobEvents(jobId, afterSeq)`
   and de-duplicates by `seq`.  This covers late subscribes, reconnects and failed publishes.
4. Stop events: `done`, `error`, `question` (job paused at `needs_input`).

The MCP shim maps events to MCP progress notifications.  MCP tool results are returned whole, so a tool call returns at
a stop event (or after `ECAN_WAIT_SECONDS`).

## Build order

1. Image index over the scraped data + a phone-photo evaluation set (~30 known products): does the right subcategory
   come back in the top 5?  This measures the riskiest part first.
2. Real intake (barcode, OCR, cut-out, attributes) + verify, our-DB lane only.
3. Web image and catalog lanes.
4. Draft + validate from the subcategory playbooks.
5. Image generation.
6. Optional remote MCP endpoint (Lambda Function URL with response streaming) for clients that can't run the shim.

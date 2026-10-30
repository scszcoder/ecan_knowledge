# Plan

Working plan for continuing this repo in a fresh Claude Code session.  Read with `README.md` (what exists and how to
use it), `docs/DESIGN.md` (the design) and `deploy/README.md` (the deploy blocker).

## Where things stand (2026-10-29)

- **Code:** the scaffold works end to end. It covers the API fields, the Step Functions workflow, the event stream and
  the MCP shim. `python -m unittest tests/test_local.py` passes, and needs no AWS.
- **Workflow steps:** all are stubs (`functions/steps.py`). They emit realistic events with placeholder data. Nothing
  identifies or drafts anything yet.
- **Deploy:** nothing is deployed, and the `readScreen` AppSync API is untouched.
  - It is blocked on a one-time admin step: the CDK bootstrap, plus the managed policy `EcanKnowledgeDeployer` attached
    to `sc` (see `deploy/README.md`). The user has asked the admin to do it.
  - `sc` itself cannot create IAM roles, write SSM parameters or manage ECR.
- **Sister repo:** `eCan_lambda/us/aws/agentScheduler/graphql/agent.graphql` holds the same additions block.
  - The change is **uncommitted** there; the user reviews and commits it.
  - Keep the two copies in sync whenever `appsync/additions.graphql` changes.

## Rules (from the user; do not relax)

1. **`readScreen` is additions only.** Never delete, rename or change an existing type, field, data source or
   resolver.
   - All schema changes go through `tools/appsync_add.py`. Run it as a dry run first, then `--apply`.
   - It refuses on any lost or changed live field.
2. **AWS:** account 667118410653, us-east-1, profile `sc`.
   - The shell has an invalid `AWS_ACCESS_KEY_ID` environment variable, so always pass `--profile sc` (or
     `boto3.Session(profile_name="sc")`).
   - `AWS_PROFILE=sc` alone is not enough, because the environment credentials win.
3. **Deploys:** the user has given permission for `cdk deploy` and `appsync_add.py --apply`, once the admin step is
   done.
   - Show the dry-run output before applying.
   - Never force a change past a refusal from `appsync_add.py`.
4. **Commits:** commit and push only when the user asks.

## Next actions, in order

### 0. Deploy the scaffold (as soon as the admin confirms)

```powershell
npx aws-cdk@2 deploy --profile sc --app ".venv\Scripts\python.exe app.py"
$env:AWS_PROFILE="sc"; .venv\Scripts\python tools\appsync_add.py          # dry run: expect exactly 12 additions
.venv\Scripts\python tools\appsync_add.py --apply
```

The `--app` value needs the backslashes. Errors such as `jsii … ENOTEMPTY` on Windows are harmless.

Then smoke-test against the real API:
- Run the shim (`shim/ecan_listing_mcp.py`) with an eCan Cognito user and `start_listing` on 2 photos.
- Expect a stream of stage events, a `needs_input` asking `target_price`, then `answer`, then `done`.
- Check that a second user cannot read or subscribe to the job.

### 1. Image index plus a phone-photo evaluation set (the riskiest part, so measure it first)

- **Data:** the scraped Amazon data comes from the eCan app's scraper skill.
  - It consists of products.json, rank_comparison and tree.md files per seed.
  - Product images sit on the NAS.
  - **The user is transferring the scraped data and the NAS images to S3.** Ask for the bucket and prefix once it is
    there, and how much has been scraped. Do not guess paths.
  - Read from that bucket directly (index and evaluation run against S3, not local copies); the stack and the Lambdas
    will need read access to it, added as a policy on our roles only.
- **Product DB:** start small. Make one table of products keyed by `parent_asin`, so variants count once, plus the
  image paths.
  - Local SQLite is fine for the experiment.
  - In the cloud it becomes Aurora Serverless v2 + pgvector, or S3 Vectors for k-nearest-neighbour search only.
- **Embeddings:** build image embeddings of each product's main image.
  - Candidates: SigLIP or CLIP running locally, or Bedrock Titan Multimodal Embeddings (Opus 5.5 is for reasoning
    and verify, not embeddings).
  - Add title-text embeddings as well.
- **Evaluation set:** about 30 phone photos of real products whose subcategory or ASIN is known, taken in normal
  lighting and on cluttered backgrounds.
  - Metrics: subcategory in the top 1 and top 5, and the exact product in the top 5.
  - Compare raw photo vs cut-out (background removed) vs cut-out + text.
- **Report:** give the numbers to the user before building further. If the subcategory top-5 hit rate is poor, fix it
  here first.

### 2. Real intake and verify, with the our-DB lane only

- **`intake`:** decode barcodes (UPC/EAN), OCR labels and packaging, remove the background and crop, and have a vision
  model extract attributes (type, material, colour, count, size).
  - Keep attribute provenance: `seller | ocr | barcode | matched:<ASIN> | inferred`, plus a confidence.
- **`lane_our_db`:** query the index from step 1 with the cut-out.
- **`verify`:** a vision model compares the seller's photos with the top candidates and returns one of `exact`,
  `variant`, `subcategory` or `unknown`.
  - `exact` means: offer on the existing ASIN, and do not create a duplicate listing.
- **Ask the seller** only for required attributes that are still missing. There is no SP-API, so the list comes from
  the specs the subcategory's winners list in the scraped data. Ask one round at most.
- **Large payloads:** keep events small. Put large outputs in S3 and reference them by key, because AppSync
  subscription messages are size-limited.

### 3. Web-image and catalog lanes

- **Web image lane:** Google Cloud Vision Web Detection and/or Google Lens via SerpAPI; 1688 or Alibaba image search
  for sourced goods.
  - Check each provider's pricing and terms first, and tell the user the cost per job.
- **Catalog lane:** no SP-API. Look up the barcode or model number with web search and in our scraped DB.
- **API keys:** store them in Secrets Manager. `sc` may not be able to create secrets; check, and if it can't, write
  the admin step into `deploy/README.md`, as was done for the bootstrap.

### 4. Draft and validate

- **Subcategory playbooks** (from the user's design):
  - title pattern, recurring keywords and the phrases winners rank for;
  - bullet themes and the specs every winner lists;
  - the price band of the top 20;
  - buyer complaints and praise from reviews;
  - an image style guide.
- **Playbook build:** compute the playbooks once per subcategory, offline.
- **Draft:** produce the title, 5 bullets, description, search terms and a price within the winners' band, and stream
  it as `draft_delta` events.
  - Never copy competitor text.
- **Validate:** no SP-API, so our own checks only: length limits, banned claims, and a similarity check against the
  matched listings.
- **LLM:** Opus 5.5 (`claude-opus-5-5`), with prompt caching on the playbook and instruction prefixes.

### 5. Images

- **Start from the seller's own photos:**
  - a white-background main image;
  - lifestyle scenes;
  - infographics with the seller's specs.
- **Never generate the product from text alone.** Use the winners' images only as a written style guide, never as
  source pictures.

### 6. Optional: remote MCP endpoint

Offer a Lambda Function URL with response streaming, for clients that cannot run the stdio shim.

## Parallel track: keyword intelligence (reverse-ASIN lookup)

Added 2026-10-30. K1–K5 are comparable in function to Helium 10's Cerebro: give one or more ASINs, get the search
keywords each one ranks for (organic and sponsored), with estimated search volume and competition metrics.  K6 (rank
tracker) and K7 (market snapshot) build on the same search-result rows and the same sales model.  It runs independently of
steps 1-5, and feeds step 4: title keywords and backend search terms come from the keywords the matched competitors
rank for.

### K1. Reverse index from the data we already have (start here, no new crawling)

- The scraper's `rank_comparison` files already record where products appear for each crawled phrase.  Once the data
  is in S3, load it into a table of one row per (keyword, date, ASIN): organic rank, sponsored rank, page, plus price,
  reviews and "bought in past month" from the same results page.
- Count per parent ASIN as well as per child ASIN.
- Query by ASIN → the reverse-ASIN table.  Query by several ASINs → shared keywords, ranked by how many of them rank
  and how high.
- Report to the user: how many keywords and ASINs are covered, per subcategory.

### K2. Keyword coverage

- Grow the keyword universe per subcategory from:
  - Amazon search autocomplete, seed + a–z / 0–9 suffixes, two levels deep;
  - related searches on results pages;
  - phrases recurring in winners' titles, bullets and reviews.
- Measure how many new keywords this adds over K1 before crawling them.
- Crawl results pages 1–3 for the new keywords with the existing scraper.  Refresh: important keywords weekly (daily
  if needed), the long tail monthly.
- Note to the user: Amazon's terms prohibit scraping, and a regular crawl schedule raises exposure and proxy cost.
  Scope is our subcategories only (tens of thousands of keywords), not the whole marketplace.

### K3. Search volume estimate (the hard part)

- No SP-API means no Brand Analytics, so volume is a model.  Signals:
  - autocomplete presence and position;
  - total "bought in past month" of page-1 products divided by an assumed click and conversion share by position;
  - number of results; sponsored-slot density.
- Calibrate against any real numbers available, for example the user's own Seller Central search-query reports, if
  the account has them.
- Label volumes as estimates.  The relative order of keywords should be reliable; absolute values may be off by about
  ±50%.  Measure that on whatever ground truth exists and report it.

### K4. Metrics

- Competing products; title density (page-1 titles containing the exact phrase).
- Sales needed to reach page 1 (estimated volume × click-through curve by position vs current page-1 sales).
- An opportunity score (volume vs competition) and rank history per ASIN and keyword.

### K5. API and MCP tool

- New additions to `readScreen` (additions only, via `tools/appsync_add.py`), e.g. a `reverseAsin(asins, filters)`
  query.  Keep `eCan_lambda`'s schema copy in sync.
- MCP tool `reverse_asin(asins, min_volume?, max_rank?, limit?)` in the shim.
- Storage: the same database as the product DB in step 1 (Aurora + pgvector), or Athena over Parquet in S3 if the
  rank history gets large.  Decide after K1 shows the data size.
- Record crawl and query costs, as for listing jobs.

### K6. Keyword tracker (daily rank tracking for chosen keywords)

Comparable in function to Helium 10's Keyword Tracker.  Needs K1's storage.

- The user picks (ASIN, keyword) pairs, their own or competitors'.  Each tracked keyword gets a guaranteed daily crawl,
  deeper than elsewhere (5–7 pages, to find ranks around 250).
- Same rows as K1 (keyword, date, ASIN, organic rank, sponsored rank).  Crawl from one fixed delivery ZIP code and
  device, and store them with each rank, since rank depends on both.
- Indexing check: search the keyword together with the ASIN and record whether the product comes back.
- Alerts: a sharp rank change or lost indexing becomes an event on a new `readScreen` subscription (additions only),
  following the listing-job event pattern; the MCP tool reports it.
- Cost grows with tracked keywords × pages × days: record it per tracked keyword and show it to the user before
  scaling up.

### K7. Market snapshot of a search-results page

Comparable in function to Helium 10's X-Ray.  Reuses K3's sales model.

- For each product on a keyword's results page: estimated monthly sales and revenue, sales rank, price, reviews,
  number of sellers, FBA or seller-fulfilled, fees, listing age; plus page totals to judge the market.
- Sales estimate: per-category curve from sales rank to sales, calibrated on products that show "bought in past
  month" (lower-bound buckets such as "1K+", so they give a floor).  Shared with K3.
- Fees: no SP-API, so compute them from Amazon's published fee tables (size tier, weight, referral rate by category).
  Keep the tables versioned with their effective date.
- Price, reviews, sellers and fulfilment come from the scraped results and product pages.
- Delivery: a `marketSnapshot(keyword)` query (additions only) and an MCP tool; later, optionally, a browser extension
  overlaying the numbers on Amazon, as a separate client of the same API.
- Feeds step 4: the subcategory playbook's price band and sales figures.

## Decisions (user, 2026-10-29)

- **No SP-API access.** The steps above already reflect this:
  - *catalog lane*: barcode/model number → web search plus our scraped DB (no `searchCatalogItems`);
  - *required attributes*: derived from the specs the subcategory's winners list in the scraped data (no Product Type
    Definitions);
  - *validate*: our own checks only (length limits, banned claims, similarity to matched listings), no
    `VALIDATION_PREVIEW`.
- **Models: Opus 5.5 (`claude-opus-5-5`) for both language and vision** (attribute extraction, verify, playbooks,
  draft).  Use prompt caching for the shared prefixes (playbook, instructions).
- **Data: the scraped data and NAS images go to S3** (the user is transferring them); everything reads from there.
- **Cost budget: none yet; measure first.**  Record per job: tokens in/out/cache-read/cache-write per step, the cost of
  every external call (image search etc.), and the total, in the job record and a `cost` event.  Report the numbers to
  the user after the first runs (and in the step 1 evaluation), then agree a budget.

## Open questions for the user

- The scraped data and NAS images are being moved to S3: which bucket and prefix, and in what layout?
  - How many seeds and subcategories?
- Who maintains the eCan app side, which calls the MCP tool and shows the result?

## Conventions

- Python 3.13 in Lambda; the local `.venv` runs Python 3.14.
- Match the existing style: small modules, few comments, and comments only where the reason isn't obvious.
- Update `tests/test_local.py` when the event contract or the shim changes, and keep it runnable without AWS.
- Update `README.md` and `docs/DESIGN.md` when behaviour changes.

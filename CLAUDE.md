# ecan_knowledge

Serverless listing agent on the existing eCan AppSync API `readScreen`.  Start with `PLAN.md` (status, rules, next
actions), then `README.md` and `docs/DESIGN.md`.

Hard rules:
- `readScreen` is **additions only**: never delete or change existing types, fields, data sources or resolvers.  Schema
  changes go through `tools/appsync_add.py` (dry run first).
- AWS: always `--profile sc` / `boto3.Session(profile_name="sc")`; the shell's `AWS_ACCESS_KEY_ID` env var is invalid.
- Keep `appsync/additions.graphql` in sync with the same block in `eCan_lambda/us/aws/agentScheduler/graphql/agent.graphql`.
- Tests: `.venv\Scripts\python -m unittest tests/test_local.py` (no AWS needed).

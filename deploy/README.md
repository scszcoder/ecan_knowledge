# Deploy permissions for `sc`

IAM policy documents may contain only `Version` and `Statement`, so the notes live here instead of in the JSON.

The CLI profiles `sc` and `maipps8` are the same IAM user (`user/sc`). It cannot create IAM roles, write SSM parameters
or manage ECR, so it cannot run `cdk bootstrap`, and it cannot deploy a stack that creates roles.  The fix is a one-time
admin step: bootstrap CDK (which creates deploy roles with the needed rights), then let `sc` use those roles.

## What `ecan-knowledge-deployer-policy.json` allows

| Statement | Why |
|---|---|
| `UseCdkBootstrapRoles` | `cdk deploy` as `sc` assumes the bootstrap roles (deploy, file publishing, lookup); CloudFormation creates the stack's roles and resources through them.  Only roles named `cdk-hnb659fds-*` in us-east-1 |
| `PassAppSyncInvokeRoleToReadScreen` | `tools/appsync_add.py` creates the `ecanKnowledgeApi` data source with the stack's AppSync invoke role; only that role, only to AppSync |
| `AddEcanKnowledgeToReadScreen` | the schema push, data sources and resolvers, limited to the `readScreen` API.  `sc` may already have these (eCan_lambda's `appsync_apply.py` uses them); listing them makes the dependency explicit |
| `ReadEcanListingStackOutputs` | `appsync_add.py` reads the stack outputs (Lambda and role ARNs) |

It grants nothing that deletes; `appsync_add.py` never removes or changes existing fields, data sources or resolvers.

## Admin steps (once, with an admin identity, e.g. the root account or an IAM admin)

From the repo root (`C:\Users\songc\PycharmProjects\ecan_knowledge`, after `pip install -r requirements.txt`):

```powershell
# 1. Remove the empty ECR repository left by sc's failed bootstrap attempt (bootstrap will recreate it)
aws ecr delete-repository --repository-name cdk-hnb659fds-container-assets-667118410653-us-east-1 --region us-east-1

# 2. Bootstrap CDK in us-east-1
npx aws-cdk@2 bootstrap aws://667118410653/us-east-1 --app ".venv\Scripts\python.exe app.py"

# 3. Give sc the deployer policy (managed: sc's inline policies are already at IAM's 2,048-character limit)
aws iam create-policy --policy-name EcanKnowledgeDeployer --policy-document file://deploy/ecan-knowledge-deployer-policy.json
aws iam attach-user-policy --user-name sc --policy-arn arn:aws:iam::667118410653:policy/EcanKnowledgeDeployer
```

Console route for step 3: IAM > Policies > Create policy > JSON (paste the file) > name it `EcanKnowledgeDeployer`;
then IAM > Users > sc > Add permissions > Attach policies directly.

## Then, as `sc`

```powershell
npx aws-cdk@2 deploy --profile sc --app ".venv\Scripts\python.exe app.py"
$env:AWS_PROFILE="sc"; .venv\Scripts\python tools\appsync_add.py            # dry run
.venv\Scripts\python tools\appsync_add.py --apply
```

The shell on the dev machine has an invalid `AWS_ACCESS_KEY_ID` environment variable; pass `--profile sc` explicitly.

## Note on the bootstrap roles

By default the bootstrap's CloudFormation execution role has `AdministratorAccess`, so anyone who can assume the deploy
role can deploy any stack.  To limit that, the admin can bootstrap with
`--cloudformation-execution-policies <policy ARN>` naming a narrower policy (Lambda, Step Functions, DynamoDB, S3, IAM
roles, Logs) instead.

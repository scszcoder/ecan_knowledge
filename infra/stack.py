"""Serverless listing agent behind the EXISTING AppSync API (readScreen): Step Functions job, Lambdas, tables, bucket.

This stack never touches the AppSync API itself.  It creates what the API's new fields need (the resolver Lambda and a
role AppSync uses to invoke it) and lets the steps publish events through the API.  tools/appsync_add.py then adds the
schema fields, data sources and resolvers, additions only.

    client ──GraphQL (Cognito)──> readScreen ──> api λ ──> Step Functions ──> step λs
    client <──subscription onListingJobEvent── readScreen <──publishListingJobEvent (IAM)── emit()
"""
from pathlib import Path

from aws_cdk import CfnOutput, Duration, RemovalPolicy, Stack
from aws_cdk import aws_iam as iam
from aws_cdk import aws_dynamodb as ddb
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as tasks
from constructs import Construct

ROOT = Path(__file__).resolve().parent.parent
RUNTIME = lambda_.Runtime.PYTHON_3_13
# the existing AppSync API ("readScreen"); override with -c appsync_api_id=... -c appsync_graphql_url=...
READSCREEN_ID = "ydusqd3wgfb6loiu2daej6qa6y"
READSCREEN_URL = "https://3oqwpjy5jzal7ezkxrxxmnt6tq.appsync-api.us-east-1.amazonaws.com/graphql"


class ListingStack(Stack):
    def __init__(self, scope: Construct, cid: str, **kw):
        super().__init__(scope, cid, **kw)

        bucket = s3.Bucket(self, "Uploads", block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
                           enforce_ssl=True, removal_policy=RemovalPolicy.RETAIN,
                           lifecycle_rules=[s3.LifecycleRule(prefix="uploads/", expiration=Duration.days(30))])
        jobs = ddb.Table(self, "Jobs", partition_key=ddb.Attribute(name="jobId", type=ddb.AttributeType.STRING),
                         billing_mode=ddb.BillingMode.PAY_PER_REQUEST, removal_policy=RemovalPolicy.RETAIN)
        events = ddb.Table(self, "Events", partition_key=ddb.Attribute(name="jobId", type=ddb.AttributeType.STRING),
                           sort_key=ddb.Attribute(name="seq", type=ddb.AttributeType.NUMBER),
                           time_to_live_attribute="expiresAt",
                           billing_mode=ddb.BillingMode.PAY_PER_REQUEST, removal_policy=RemovalPolicy.RETAIN)

        api_id = self.node.try_get_context("appsync_api_id") or READSCREEN_ID
        graphql_url = self.node.try_get_context("appsync_graphql_url") or READSCREEN_URL
        publish_arn = f"arn:aws:appsync:{self.region}:{self.account}:apis/{api_id}/types/Mutation/fields/publishListingJobEvent"

        env = {"JOBS_TABLE": jobs.table_name, "EVENTS_TABLE": events.table_name, "APPSYNC_GRAPHQL_URL": graphql_url}
        code = lambda_.Code.from_asset(str(ROOT / "functions"), exclude=["__pycache__"])

        def fn(name: str, timeout: int = 60) -> lambda_.Function:
            f = lambda_.Function(self, f"Step-{name}", runtime=RUNTIME, handler=f"steps.{name}", code=code,
                                 timeout=Duration.seconds(timeout), memory_size=512, environment=env)
            jobs.grant_read_write_data(f)
            events.grant_read_write_data(f)
            f.add_to_role_policy(iam.PolicyStatement(actions=["appsync:GraphQL"], resources=[publish_arn]))
            bucket.grant_read_write(f)
            return f

        def step(name: str, timeout: int = 60, **kw) -> tasks.LambdaInvoke:
            return tasks.LambdaInvoke(self, name, lambda_function=fn(name, timeout), payload_response_only=True,
                                      retry_on_service_exceptions=True, **kw)

        # --- the job
        lanes = (sfn.Parallel(self, "identify", result_path="$.lanes")
                 .branch(step("lane_catalog")).branch(step("lane_web_image")).branch(step("lane_our_db")))
        ask = tasks.LambdaInvoke(self, "ask_seller", lambda_function=fn("ask_seller"),
                                 integration_pattern=sfn.IntegrationPattern.WAIT_FOR_TASK_TOKEN,
                                 payload=sfn.TaskInput.from_object({"taskToken": sfn.JsonPath.task_token,
                                                                    "state": sfn.JsonPath.entire_payload}),
                                 result_path="$.answers", task_timeout=sfn.Timeout.duration(Duration.days(7)))
        finish = step("finish")
        compose = step("draft", 300).next(step("validate", 120)).next(step("images", 300)).next(finish)
        ask.next(compose)
        decide = (sfn.Choice(self, "decide")
                  .when(sfn.Condition.string_equals("$.match.level", "exact"), finish)
                  .when(sfn.Condition.is_present("$.questions[0]"), ask)
                  .otherwise(compose))
        body = step("intake", 120).next(lanes).next(step("verify", 120)).next(decide)
        run = sfn.Parallel(self, "run").branch(body)
        run.add_catch(step("fail"), result_path="$.error")
        machine = sfn.StateMachine(self, "ListingJob", definition_body=sfn.DefinitionBody.from_chainable(run),
                                   timeout=Duration.days(8))

        # --- resolver for the new fields on the existing API (attached by tools/appsync_add.py)
        resolver = lambda_.Function(self, "Api", runtime=RUNTIME, handler="api.handler", code=code,
                                    timeout=Duration.seconds(25), memory_size=256,
                                    environment={**env, "UPLOAD_BUCKET": bucket.bucket_name,
                                                 "STATE_MACHINE_ARN": machine.state_machine_arn})
        jobs.grant_read_write_data(resolver)
        events.grant_read_data(resolver)
        bucket.grant_put(resolver)
        machine.grant_start_execution(resolver)
        machine.grant_task_response(resolver)
        appsync_role = iam.Role(self, "AppSyncInvoke", assumed_by=iam.ServicePrincipal("appsync.amazonaws.com"),
                                description="readScreen data source ecanKnowledgeApi invokes the ecan_knowledge resolver")
        resolver.grant_invoke(appsync_role)

        CfnOutput(self, "ApiFunctionArn", value=resolver.function_arn)
        CfnOutput(self, "AppSyncRoleArn", value=appsync_role.role_arn)
        CfnOutput(self, "UploadBucket", value=bucket.bucket_name)

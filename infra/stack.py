"""Serverless listing agent: AppSync GraphQL (requests) + AppSync Events (stream) + Step Functions (the job).

    client ──GraphQL──> AppSync ──> api λ ──> Step Functions ──> step λs ──emit──> DynamoDB events + Events /jobs/{id}
    client <──WebSocket── AppSync Events /jobs/{jobId}
"""
from pathlib import Path

from aws_cdk import CfnOutput, Duration, Expiration, RemovalPolicy, Stack
from aws_cdk import aws_appsync as appsync
from aws_cdk import aws_dynamodb as ddb
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as tasks
from constructs import Construct

ROOT = Path(__file__).resolve().parent.parent
RUNTIME = lambda_.Runtime.PYTHON_3_13


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

        # --- stream: AppSync Events.  Clients subscribe with the API key; only the step functions (IAM) publish.
        api_key = appsync.AppSyncAuthProvider(authorization_type=appsync.AppSyncAuthorizationType.API_KEY)
        iam = appsync.AppSyncAuthProvider(authorization_type=appsync.AppSyncAuthorizationType.IAM)
        stream = appsync.EventApi(self, "Stream", api_name=f"{cid}-stream", authorization_config=appsync.EventApiAuthConfig(
            auth_providers=[api_key, iam],
            connection_auth_mode_types=[appsync.AppSyncAuthorizationType.API_KEY, appsync.AppSyncAuthorizationType.IAM],
            default_publish_auth_mode_types=[appsync.AppSyncAuthorizationType.IAM],
            default_subscribe_auth_mode_types=[appsync.AppSyncAuthorizationType.API_KEY]))
        stream.add_channel_namespace("jobs")

        env = {"JOBS_TABLE": jobs.table_name, "EVENTS_TABLE": events.table_name, "EVENTS_HTTP_DNS": stream.http_dns}
        code = lambda_.Code.from_asset(str(ROOT / "functions"), exclude=["__pycache__"])

        def fn(name: str, timeout: int = 60) -> lambda_.Function:
            f = lambda_.Function(self, f"Step-{name}", runtime=RUNTIME, handler=f"steps.{name}", code=code,
                                 timeout=Duration.seconds(timeout), memory_size=512, environment=env)
            jobs.grant_read_write_data(f)
            events.grant_read_write_data(f)
            stream.grant_publish(f)
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

        # --- requests: AppSync GraphQL with a single Lambda resolver
        resolver = lambda_.Function(self, "Api", runtime=RUNTIME, handler="api.handler", code=code,
                                    timeout=Duration.seconds(25), memory_size=256,
                                    environment={**env, "UPLOAD_BUCKET": bucket.bucket_name,
                                                 "STATE_MACHINE_ARN": machine.state_machine_arn})
        jobs.grant_read_write_data(resolver)
        events.grant_read_data(resolver)
        bucket.grant_put(resolver)
        machine.grant_start_execution(resolver)
        machine.grant_task_response(resolver)

        gql = appsync.GraphqlApi(self, "Graphql", name=f"{cid}-api",
                                 definition=appsync.Definition.from_file(str(ROOT / "api" / "schema.graphql")),
                                 authorization_config=appsync.AuthorizationConfig(
                                     default_authorization=appsync.AuthorizationMode(
                                         authorization_type=appsync.AuthorizationType.API_KEY,
                                         api_key_config=appsync.ApiKeyConfig(expires=Expiration.after(Duration.days(365))))))
        ds = gql.add_lambda_data_source("api", resolver)
        for type_name, field in [("Query", "listing"), ("Query", "events"), ("Mutation", "createUploadUrls"),
                                 ("Mutation", "startListing"), ("Mutation", "answer")]:
            ds.create_resolver(f"{type_name}-{field}", type_name=type_name, field_name=field)

        # --- what the MCP shim needs (ECAN_* environment variables)
        CfnOutput(self, "GraphqlUrl", value=gql.graphql_url)
        CfnOutput(self, "GraphqlApiKey", value=gql.api_key or "")
        CfnOutput(self, "EventsHttpDns", value=stream.http_dns)
        CfnOutput(self, "EventsRealtimeDns", value=stream.realtime_dns)
        CfnOutput(self, "EventsApiKey", value=stream.api_keys["Default"].attr_api_key)

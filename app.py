import os

import aws_cdk as cdk

from infra.stack import ListingStack

app = cdk.App()
ListingStack(app, app.node.try_get_context("stack_name") or "EcanListing",
             env=cdk.Environment(account=os.environ.get("CDK_DEFAULT_ACCOUNT"), region="us-east-1"))  # region of readScreen
app.synth()

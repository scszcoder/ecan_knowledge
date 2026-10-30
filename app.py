import aws_cdk as cdk

from infra.stack import ListingStack

app = cdk.App()
ListingStack(app, app.node.try_get_context("stack_name") or "EcanListing")
app.synth()

from behave import *
from featureform import register_spark


@when("I register Spark")
def step_impl(context):
    context.spark_name = "test_spark"
    try:
        context.spark = register_spark(
            name=context.spark_name,
            executor=context.spark_credentials,
            filestore=context.cloud_credentials,
        )
    except Exception as e:
        context.exception = e

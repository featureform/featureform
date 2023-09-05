from behave import *
from featureform import register_spark
import featureform as ff
import os


@when("I create a user")
def step_impl(context):
    context.exception = None
    try:
        ff.register_user("test_user")
        context.client.apply()
    except Exception as e:
        context.exception = e

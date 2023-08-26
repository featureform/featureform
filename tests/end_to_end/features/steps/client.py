from behave import *


@given("Featureform is installed")
def step_impl(context):
    import featureform

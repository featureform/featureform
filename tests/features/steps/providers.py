from behave import *
import featureform as ff


@when("we register localmode")
def step_impl(context):
    ff.register_local()
    context.client.apply()


@when("we register postgres")
def step_impl(context):
    ff.register_postgres()
    context.client.apply()

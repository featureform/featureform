from behave import *


@when('I get the provider with name "{name}"')
def step_impl(context, name):
    context.filestore_name = name
    context.filestore = context.client.get_provider(name)

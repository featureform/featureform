from behave import step


@step("I can get the offline provider")
def step_impl(context):
    context.offline_provider = context.client.get_provider(context.offline_provider_name)


@step("I can get the online provider")
def step_impl(context):
    context.online_provider = context.client.get_provider(context.online_provider_name)

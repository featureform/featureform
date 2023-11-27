from behave import *
import featureform as ff


@when("I register an ondemand feature")
def step_impl(context):
    @ff.ondemand_feature
    def test_feature(client, params, entity):
        return 1

    context.client.apply()


@then("I can pull the ondemand feature")
def step_impl(context):
    value = context.client.features([("test_feature", ff.get_run())], {})
    assert value[0] == 1

from behave import *
import featureform as ff


@given("The Snowflake env variables are available")
def step_impl(context):
    context.snowflake_username = os.getenv("SNOWFLAKE_USERNAME", "")
    context.snowflake_password = os.getenv("SNOWFLAKE_PASSWORD", "")
    context.snowflake_account = os.getenv("SNOWFLAKE_ACCOUNT", "")
    context.snowflake_organization = os.getenv("SNOWFLAKE_ORG", "")

    if context.snowflake_username == "":
        raise Exception("Snowflake uername is not set")
    if context.snowflake_password == "":
        raise Exception("Snowflake password is not set")
    if context.snowflake_account == "":
        raise Exception("Snowflake account is not set")
    if context.snowflake_organization == "":
        raise Exception("Snowflake organization is not set")


@when("I register Snowflake")
def step_impl(context):
    try:
        context.snowflake = context.featureform.register_snowflake(
            name="test_snowflake",
            description="Offline store",
            team="Featureform",
            username=context.snowflake_username,
            password=context.snowflake_password,
            account=context.snowflake_account,
            organization=context.snowflake_organization,
            database="0884D0DD-468D-4C3A-8109-3C2BAAD72EF7",
            schema="PUBLIC",
        )
    except Exception as e:
        context.exception = e


@when("I register the tables from the database")
def step_impl(context):
    context.boolean_table = context.snowflake.register_table(
        name="boolean_table",
        table="featureform_resource_feature__8a49dead-41a6-48c39ee7-7ed75d783fe4__0efdd949-f967-4796-8ed3-6c6f736344e8",
    )
    context.number_table = context.snowflake.register_table(
        name="number_table",
        table="featureform_resource_feature__f4d42278-0889-44d7-9928-8aef22d23c16__6a6e8ff4-8a8f-4217-8096-bb360ae1e99b",
    )


@then("I serve batch features")
def step_impl(context):
    try:
        context.iter = context.client.batch_features(
            ("table1_feature", ff.get_run()),
            ("table2_feature", ff.get_run()),
            ("table3_feature", ff.get_run()),
            ("table4_feature", ff.get_run()),
        )
    except Exception as e:
        context.exception = e
        return
    context.exception = None


@then("I can get a list containing the entity name and a tuple with all the features")
def step_impl(context):
    try:
        for row in context.iter:
            assert len(row) == 2
    except Exception as e:
        context.exception = e
        return
    context.exception = None


@when("I define a User and register features")
def step_impl(context):
    class User:
        context.boolean_feature = context.featureform.Feature(
            context.boolean_table[["entity", " value", "ts"]],
            variant="batch_serving_test_15",
            type=context.featureform.Float32,
            inference_store=context.redis,
        )
        context.numerical_feature = context.featureform.Feature(
            context.number_table[["entity", " value", "ts"]],
            variant="batch_serving_test_15",
            type=context.featureform.Float32,
            inference_store=context.redis,
        )
        context.string_feature = context.featureform.Feature(
            context.number_table[["entity", " value", "ts"]],
            variant="batch_serving_test_15",
            type=context.featureform.String,
            inference_store=context.redis,
        )

    context.featureform.entity(User)

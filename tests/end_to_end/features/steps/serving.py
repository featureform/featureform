from behave import *
import featureform as ff

    # And I apply "snowflake_config.py" file with a "hosted" "insecure" CLI
    # And I create a "hosted" "insecure" client for "localhost:7878" (should be done)
    # And I apply the "serve_batch_features.py" file
    # Then I can iterate through the rows of the batch feature table
    # And I can get a list containing the entity name and a tuple with all the features

@when("I register Snowflake")
def step_impl(context):
    context.snowflake_name = "test_snowflake"
    try:
        context.snowflake = context.featureform.register_snowflake(
            name = context.snowflake_name,
            description = "Offline store",
            team = context.snowflake_team,
            username = context.snowflake_username,
            password = context.snowflake_password,
            account = context.snowflake_account,
            organization = context.snowflake_organization,
            database = "0884D0DD-468D-4C3A-8109-3C2BAAD72EF7",
            schema = "PUBLIC",
        )
    except Exception as e:
        context.exception = e

@when('I register the tables from the database')
def step_impl(context):
    context.boolean_table = context.snowflake.register_table(
    name="boolean_table",
    table= "featureform_resource_feature__8a49dead-41a6-48c39ee7-7ed75d783fe4__0efdd949-f967-4796-8ed3-6c6f736344e8",
    )
    context.number_table = context.snowflake.register_table(
    name="number_table",
    table="featureform_resource_feature__f4d42278-0889-44d7-9928-8aef22d23c16__6a6e8ff4-8a8f-4217-8096-bb360ae1e99b",
    )


@then("I serve batch features")
def step_impl(context):
    try:
        context.iter = context.client.iterate_feature_set(("table1_feature", ff.get_run()), ("table2_feature", ff.get_run()), ("table3_feature",ff.get_run()), ("table4_feature", ff.get_run()))
    except Exception as e:
        context.exception = e
        return
    context.exception = None

@then("I can get a list containing the entity name and a tuple with all the features")
def step_impl(context):
    try:
        for row in context.iter:
            assert (len(row) == 2)
    except Exception as e:
        context.exception = e
        return
    context.exception = None


@when("I define a User and register features")
def step_impl(context):
    class User:
        context.boolean_feature = context.featureform.Feature(
            context.boolean_table[['entity',' value', 'ts']],
            variant="batch_serving_test_15",
            type=context.featureform.Float32,
            inference_store=context.redis,
        )
        context.numerical_feature = context.featureform.Feature(
            context.number_table[['entity',' value', 'ts']],
            variant="batch_serving_test_15",
            type=context.featureform.Float32,
            inference_store=context.redis,
        )
        context.string_feature = context.featureform.Feature(
            context.number_table[['entity',' value', 'ts']],
            variant="batch_serving_test_15",
            type=context.featureform.String,
            inference_store=context.redis,
        )

    context.featureform.entity(User)


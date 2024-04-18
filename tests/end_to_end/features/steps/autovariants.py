import random

import featureform as ff
from behave import when, then


@when("I register a transformation with auto variant")
def step_impl(context):
    @context.spark.df_transformation(
        inputs=[context.transactions],
    )
    def some_transformation(df):
        """Unedited transactions"""
        return df

    context.client.apply(asynchronous=False, verbose=True)
    context.transformation = some_transformation


@then("I should be able to reuse the same variant for the same transformation")
def step_impl(context):
    @context.spark.df_transformation(
        inputs=[context.transactions],
    )
    def some_transformation(df):
        """Unedited transactions"""
        return df

    context.apply(asynchronous=False, verbose=True)
    assert some_transformation.name == context.transformation.name
    assert some_transformation.variant == context.transformation.variant

    context.new_transformation = some_transformation


@then("I can get the transformation as df")
def step_impl(context):
    df = context.client.dataframe(context.new_transformation)
    assert df is not None
    assert df.count() > 0


@when("I register a transformation with user-provided variant")
def step_impl(context):
    new_random_variant = f"v{str(random.randint(10000, 99999))}"

    @context.spark.df_transformation(
        variant=new_random_variant,
        inputs=[context.transactions],
    )
    def some_transformation(df):
        """Unedited transactions"""
        return df

    context.client.apply(asynchronous=False, verbose=True)

    assert some_transformation.name == context.transformation.name
    assert some_transformation.variant != context.transformation.variant
    assert some_transformation.variant == new_random_variant


@then("I should be able to register a modified transformation with new auto variant")
def step_impl(context):
    @context.spark.df_transformation(
        inputs=[context.transactions],
    )
    def some_transformation(df):
        """modified transactions"""
        df = df.withColumn("new_id", ff.F.col("TransactionID") + 1)
        return df

    context.client.apply(asynchronous=False, verbose=True)
    context.new_transformation = some_transformation

    assert context.new_transformation.name == context.transformation.name
    assert context.new_transformation.variant != context.transformation.variant

import os

import featureform as ff
from behave import given, when, then


@when("I register postgres")
def step_impl(context):
    context.postgres = ff.register_postgres(
        name="postgres-quickstart",
        host="172.17.0.1",  # The docker dns name for postgres
        port="5432",
        user="postgres",
        password="password",
        database="postgres",
    )


@when("I register a table from postgres")
def step_impl(context):
    context.transactions = context.postgres.register_table(
        name="transactions",
        variant=ff.get_run(),
        table="transactions",  # This is the table's name in Postgres
    )


@when("I create a dataframe from a serving client")
def step_impl(context):
    context.dataset_df = context.client.dataframe(context.transactions)


@then("I define a User and register multiple features excluding one")
def step_impl(context):
    @ff.entity
    class User:
        all_features = ff.MultiFeature(
            dataset=context.transactions,
            df=context.dataset_df,
            variant=ff.get_run(),
            exclude_columns=["transactionamount"],
            entity_column="customerid",
            timestamp_column="timestamp",
            inference_store=context.redis,
        )

    context.client.apply()


@then(
    "I define a User and register multiple but not all features, with no timestamp column"
)
def step_impl(context):
    @ff.entity
    class User:
        all_features = ff.MultiFeature(
            dataset=context.transactions,
            df=context.dataset_df,
            variant=ff.get_run(),
            include_columns=[
                "transactionamount",
                "customerdob",
                "custaccountbalance",
                "custlocation",
            ],
            entity_column="customerid",
            inference_store=context.redis,
        )

    context.client.apply()


@then("I should be able to serve a batch of features")
def step_impl(context):
    # Serve batch features
    batch_features = context.client.batch_features(
        [
            ("customerdob", ff.get_run()),
            ("custaccountbalance", ff.get_run()),
            ("custlocation", ff.get_run()),
        ]
    )

    for entity, features in batch_features:
        assert len(features) == 3
        assert entity != "" and entity != None
        break

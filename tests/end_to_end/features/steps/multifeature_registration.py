import os

import featureform as ff
from behave import given, when, then


@when("I register postgres")
def step_impl(context):
    try:
        context.postgres = ff.register_postgres(
            name="postgres-quickstart",
            host="host.docker.internal",  # The docker dns name for postgres
            port="5432",
            user="postgres",
            password="",
            database="",
        )
    except Exception as e:
        context.exception = e


@when("I register a table from postgres")
def step_impl(context):
    context.transactions = context.postgres.register_table(
        name="transactions",
        variant="v1",
        table="transactions",  # This is the table's name in Postgres
    )


@when("I create a dataframe from a serving client")
def step_impl(context):
    context.client = ff.Client(host="localhost:7878", insecure=True)
    context.dataset_df = context.client.dataframe(context.transactions)


@then("I define a User and register multiple features excluding one")
def step_impl(context):
    @ff.entity
    class User:
        all_features = ff.MultiFeature(
            dataset=context.transactions,
            df=context.dataset_df,
            variant="version_1",
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
            variant="version_1",
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
            ("customerdob", "version_1"),
            ("custaccountbalance", "version_1"),
            ("custlocation", "version_1"),
        ]
    )

    for (entity, features) in batch_features:
        assert len(features) == 3
        assert entity != "" and entity is not None
        break
        

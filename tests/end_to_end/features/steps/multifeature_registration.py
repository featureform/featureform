import os

import featureform as ff
from behave import given, when, then


@when("I register postgres")
def step_impl(context):
    context.postgres_password = os.getenv("POSTGRES_PASSWORD", "")
    context.postgres_database = os.getenv("POSTGRES_DB", "")

    if context.postgres_password == "":
        raise Exception("Postgres password is not set")
    if context.postgres_database == "":
        raise Exception("Postgres database is not set")

    try:
        context.postgres = ff.register_postgres(
            name="postgres-quickstart",
            host="host.docker.internal",  # The docker dns name for postgres
            port="5432",
            user="postgres",
            password=context.postgres_password,
            database=context.postgres_database,
        )
    except Exception as e:
        context.exception = e


@when("I register a table from postgres")
def step_impl(context):
    context.transactions = context.postgres.register_table(
        name="transactions",
        variant="v4",
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
            variant="version_3",
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
            variant="version_3",
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
            ("customerdob", "version_2"),
            ("custaccountbalance", "version_2"),
            ("custlocation", "version_2"),
        ]
    )

    for entity, features in batch_features:
        assert len(features) == 3
        assert entity != "" and entity != None
        break

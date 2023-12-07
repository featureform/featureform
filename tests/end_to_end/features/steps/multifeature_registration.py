import os
import random

import requests
from behave import *
import featureform as ff

@when("I register postgres")
def step_impl(context):
    try:
        context.postgres = context.featureform.register_postgres(
            name="postgres-quickstart",
            host="host.docker.internal",  # The docker dns name for postgres
            port="5432",
            user=context.POSTGRES_USER,
            password=context.POSTGRES_PASSWORD,
            database="postgres",
        )
    except Exception as e:
        context.exception = e

@when("I register a table from postgres")
def step_impl(context):
    context.transactions = context.postgres.register_table(
        name="transactions",
        variant=f"variant_multifeature",
        table="transactions",  # This is the table's name in Postgres
    )
@when("I create a dataframe from a serving client")
def step_impl(context):
    context.client = context.featureform.Client(host="localhost:7878", insecure=True)
    context.dataset_df = context.client.dataframe(context.transactions)


@then("I define a User and register multiple features excluding one")
def step_impl(context):
    class User:
        context.all_features = context.featureform.MultiFeature(dataset=context.transactions, df=context.dataset_df, variant="default", exclude_columns=["transactionamount"], entity_column="customerid", timestamp_column="timestamp", inference_store=context.redis)

@then("I define a User and register multiple but not all features, with no timestamp column")
def step_impl(context):
    class User:
        context.all_features = context.featureform.MultiFeature(dataset=context.transactions, df=context.dataset_df, variant="default", include_columns=["transactionamount", "customerdob", "custaccountbalance", "custlocation"], entity_column="customerid", inference_store=context.redis)

@then("I should be able to serve a batch of features")
def step_impl(context):
    serving = context.featureform.ServingClient(host="localhost:7878", insecure=True)

    # Serve batch features
    batch_features = serving.batch_features(
        ("customerdob", "default"),
        ("custaccountbalance", "default"),
        ("custlocation", "default"),
    )

    for entity, features in batch_features:
        print(entity)
        print(features)

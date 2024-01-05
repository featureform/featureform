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
            exclude_columns=["transactionamount"],
            entity_column="customerid",
            timestamp_column="timestamp",
            inference_store=context.redis,
        )

    context.multifeature = User.all_features

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
            variant=context.run_name,
            include_columns=[
                "transactionamount",
                "customerdob",
                "custaccountbalance",
                "custlocation",
            ],
            entity_column="customerid",
            inference_store=context.redis,
        )

    context.multifeature = User.all_features

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
    expected_output = [
        ["C1010245", ["6/11/87", 4668.96, "CHENNAI"]],
        ["C1011072", ["20/8/89", 128634.09, "PANCHKULA"]],
        ["C1011148", ["25/12/84", 2454.7, "BANGLORE"]],
        ["C1011359", ["5/6/94", 179.99, "CHENNAI"]],
        ["C1011541", ["21/8/89", 230.31, "AHMEDABAD"]],
        ["C1011935", ["4/3/87", 14478.55, "BANGALORE"]],
        ["C1012018", ["5/2/71", 920.07, "T MHALAUGE TEHSIL AMBEGAV PUNE"]],
        ["C1012421", ["16/11/88", 3219.15, "GURGAON"]],
        ["C1012688", ["12/1/89", 59759.73, "OLD SANGVI"]],
        ["C1012767", ["28/10/83", 53531.42, "THANE"]],
    ]
    count = 0
    for entity, features in batch_features:
        assert len(features) == 3
        assert entity != "" and entity != None
        assert [
            entity,
            features,
        ] in expected_output, (
            f"[{entity}, {features}] does not exist in expected_output"
        )
        count += 1
        if count == 10:
            break
    batch_features.restart()

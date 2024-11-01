#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

import pytest
import featureform as ff

# The markers can be used to identify to run a subset of tests
# pytest -m snowflake
pytestmark = [pytest.mark.snowflake]


def test_snowflake_primary_dataset_registration(
    ff_client, snowflake_fixture, num_records_limit
):
    transactions = snowflake_fixture.register_table(
        name="snowflake_transactions",
        table="TRANSACTIONS",
    )

    ff_client.apply(asynchronous=False, verbose=True)

    df = ff_client.dataframe(transactions, limit=num_records_limit)

    assert len(df) == num_records_limit


def test_snowflake_sql_transformation(
    ff_client, snowflake_fixture, snowflake_transactions_dataset, num_records_limit
):
    @snowflake_fixture.sql_transformation(inputs=[snowflake_transactions_dataset])
    def snowflake_sql_transformation(tbl):
        return (
            "SELECT CustomerID AS user_id, avg(TransactionAmount) "
            "AS avg_transaction_amt FROM {{ tbl }} GROUP BY user_id"
        )

    ff_client.apply(asynchronous=False, verbose=True)

    df = ff_client.dataframe(snowflake_sql_transformation, limit=num_records_limit)

    assert len(df) == num_records_limit


def test_snowflake_sql_transformation_ff_last_run_timestamp(
    ff_client, snowflake_fixture, snowflake_transactions_dataset, num_records_limit
):
    @snowflake_fixture.sql_transformation(inputs=[snowflake_transactions_dataset])
    def snowflake_variable_transformation(table):
        return "SELECT CUSTOMERID FROM {{ table }} WHERE CAST(timestamp AS TIMESTAMP_NTZ(6)) > CAST(FF_LAST_RUN_TIMESTAMP AS TIMESTAMP_NTZ(6))"

    ff_client.apply()
    df = ff_client.dataframe(snowflake_variable_transformation, limit=num_records_limit)

    assert len(df) == num_records_limit


def test_snowflake_feature_label_registration(
    ff_client, redis_fixture, snowflake_fixture, snowflake_transactions_dataset
):
    @snowflake_fixture.sql_transformation(inputs=[snowflake_transactions_dataset])
    def snowflake_avg_transactions(tbl):
        return (
            "SELECT CustomerID AS user_id, avg(TransactionAmount) "
            "AS avg_transaction_amt FROM {{ tbl }} GROUP BY user_id"
        )

    @ff.entity
    class SnowflakeUser:
        avg_transactions = ff.Feature(
            snowflake_avg_transactions[["user_id", "avg_transaction_amt"]],
            type=ff.Float32,
            inference_store=redis_fixture,
        )
        fraudulent = ff.Label(
            snowflake_transactions_dataset[["CustomerID", "IsFraud"]], type=ff.Bool
        )

    ff_client.apply(asynchronous=False, verbose=True)

    feature = ff_client.features(
        [SnowflakeUser.avg_transactions],
        {"snowflakeuser": "C6846834"},
    )
    assert feature[0] == 1230


def test_snowflake_training_set_registration(
    ff_client, redis_fixture, snowflake_fixture, snowflake_transactions_dataset
):
    @snowflake_fixture.sql_transformation(inputs=[snowflake_transactions_dataset])
    def snowflake_avg_transactions_ts(tbl):
        return (
            "SELECT CustomerID AS user_id, avg(TransactionAmount) "
            "AS avg_transaction_amt FROM {{ tbl }} GROUP BY user_id"
        )

    @ff.entity
    class SnowflakeUser:
        avg_transactions_ts = ff.Feature(
            snowflake_avg_transactions_ts[["user_id", "avg_transaction_amt"]],
            type=ff.Float32,
            inference_store=redis_fixture,
        )
        fraudulent_ts = ff.Label(
            snowflake_transactions_dataset[["CustomerID", "IsFraud"]], type=ff.Bool
        )

    training_set = ff.register_training_set(
        name="snowflake_training_set",
        features=[SnowflakeUser.avg_transactions_ts],
        label=SnowflakeUser.fraudulent_ts,
    )

    ff_client.apply(asynchronous=False, verbose=True)

    ts = ff_client.training_set(training_set)
    df = ts.dataframe()
    assert len(df) == 960794


@pytest.mark.parametrize(
    "db, schema, table",
    [
        ("DEMO", "TEST", "TRANSACTIONS2"),
        ("DEMO2", "PUBLIC", "TRANSACTIONS"),
    ],
)
def test_snowflake_register_primary_dataset_different_db_schema(
    ff_client,
    snowflake_fixture,
    num_records_limit,
    db,
    schema,
    table,
):
    transactions = snowflake_fixture.register_table(
        name=f"snowflake_trx_{db.lower()}_{schema.lower()}_{table.lower()}",
        table=table,
        database=db,
        schema=schema,
    )

    ff_client.apply(asynchronous=False, verbose=True)

    df = ff_client.dataframe(transactions, limit=num_records_limit)

    assert len(df) == int(num_records_limit)

    @snowflake_fixture.sql_transformation(inputs=[transactions])
    def snowflake_sql_diff_schema(tbl):
        return "SELECT CUSTOMERID FROM {{ tbl }} LIMIT 900"

    ff_client.apply(asynchronous=False, verbose=True)

    df = ff_client.dataframe(snowflake_sql_diff_schema)
    assert len(df) == 900

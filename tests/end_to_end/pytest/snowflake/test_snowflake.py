#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

import pytest
import featureform as ff
import time

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


def test_snowflake_multi_entity_label_registration(
    ff_client, snowflake_fixture, snowflake_transactions_dataset
):
    @snowflake_fixture.sql_transformation(inputs=[snowflake_transactions_dataset])
    def snowflake_avg_transactions(tbl):
        return (
            "SELECT CustomerID AS user_id, CustomerID, avg(TransactionAmount) "
            "AS avg_transaction_amt, IsFraud FROM {{ tbl }} GROUP BY user_id, IsFraud"
        )

    @ff.entity
    class SnowflakeUser:
        pass

    @ff.entity
    class SnowflakeCustomer:
        pass

    snowflake_fixture.register_label(
        name="fraudulent",
        entity_mappings=[
            {"entity": "snowflakeuser", "column": "user_id"},
            {"entity": "snowflakecustomer", "column": "CustomerID"},
        ],
        value_type=ff.Bool,
        dataset=snowflake_avg_transactions,
        value_column="IsFraud",
    )

    variant = ff.get_run()

    ff_client.apply(asynchronous=False, verbose=True)

    label = ff_client.get_label("fraudulent", variant)

    assert isinstance(label.location, ff.EntityMappings)
    assert len(label.location.mappings) == 2
    assert label.location == ff.EntityMappings(
        mappings=[
            ff.EntityMapping(name="snowflakeuser", entity_column="user_id"),
            ff.EntityMapping(name="snowflakecustomer", entity_column="CustomerID"),
        ],
        value_column="IsFraud",
    )


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
    assert len(df) == 1048124


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


def table_exists(snowflake_connector, location, retries=3, delay=0.5):
    """
    Check if a table exists in Snowflake with custom retries.

    Args:
        snowflake_connector: The Snowflake connection object.
        location: The table name to check.
        retries: Number of retry attempts.
        delay: Delay between retries in seconds.

    Returns:
        bool: True if the table exists, False otherwise.
    """
    for attempt in range(retries):
        try:
            cs = snowflake_connector.cursor()
            query = f"SELECT COUNT(*) FROM information_schema.tables WHERE TABLE_NAME = '{location}'"
            cs.execute(query)
            result = cs.fetchone()
            return result[0] > 0
        except Exception as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            if attempt < retries - 1:
                time.sleep(delay)  # Wait before the next retry
        finally:
            cs.close()
    return False


@pytest.mark.skip(reason="Flaky right now due to timing issues")
def test_prune_snowflake_transformation(
    ff_client,
    snowflake_fixture,
    snowflake_transactions_dataset,
    snowflake_connector_fixture,
):
    """
    Test pruning a Snowflake SQL transformation and verify that the table is deleted.
    """

    @snowflake_fixture.sql_transformation(inputs=[snowflake_transactions_dataset])
    def snowflake_sql_transformation(tbl):
        return (
            "SELECT CustomerID AS user_id, AVG(TransactionAmount) "
            "AS avg_transaction_amt FROM {{ tbl }} GROUP BY user_id"
        )

    ff_client.apply(asynchronous=False, verbose=True)

    # Get location of the transformation
    loc = ff_client.location(snowflake_sql_transformation)

    # Prune the transformation
    ff_client.prune(snowflake_sql_transformation)

    time.sleep(2)
    # Assert table no longer exists (retries handle timing issues)
    assert not table_exists(snowflake_connector_fixture, loc)


@pytest.mark.skip(reason="Flaky right now due to timing issues")
def test_prune_snowflake_label_registration(
    ff_client,
    snowflake_transactions_dataset,
    snowflake_connector_fixture,
):
    """
    Test pruning a Snowflake label registration and verify that the table is deleted.
    """

    @ff.entity
    class SnowflakeUser:
        delete_fraudulent = ff.Label(
            snowflake_transactions_dataset[["CustomerID", "IsFraud"]], type=ff.Bool
        )

    ff_client.apply(asynchronous=False, verbose=True)

    # Get location of the label
    label_loc = ff_client.location(SnowflakeUser.delete_fraudulent)

    # Prune the label
    ff_client.prune(SnowflakeUser.delete_fraudulent)

    time.sleep(2)
    # Assert table no longer exists (retries handle timing issues)
    assert not table_exists(snowflake_connector_fixture, label_loc)

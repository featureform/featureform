#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

import os

import pytest
import featureform as ff
import snowflake.connector


@pytest.fixture(scope="module")
def snowflake_fixture(client):
    snowflake = ff.register_snowflake(
        name="snowflake-fixture",
        username=os.getenv("SNOWFLAKE_USERNAME", None),
        password=os.getenv("SNOWFLAKE_PASSWORD", None),
        account=os.getenv("SNOWFLAKE_ACCOUNT", None),
        organization=os.getenv("SNOWFLAKE_ORG"),
        database="DEMO",
        schema="PUBLIC",
        warehouse="COMPUTE_WH",
        catalog=ff.SnowflakeCatalog(
            external_volume=os.getenv("SNOWFLAKE_EXTERNAL_VOLUME", None),
            base_location=os.getenv("SNOWFLAKE_BASE_LOCATION", None),
        ),
    )

    client.apply()

    return snowflake


@pytest.fixture(scope="module")
def snowflake_transactions_dataset(client, snowflake_fixture):
    transactions = snowflake_fixture.register_table(
        name="snowflake_transactions",
        table="TRANSACTIONS",
    )

    client.apply(asynchronous=False, verbose=True)

    return transactions


@pytest.fixture(scope="module")
def snowflake_connector_fixture():
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USERNAME", None),
        password=os.getenv("SNOWFLAKE_PASSWORD", None),
        account=f"{os.getenv('SNOWFLAKE_ORG', None)}-{os.getenv('SNOWFLAKE_ACCOUNT', None)}",
        warehouse="COMPUTE_WH",
        database="DEMO",
        schema="PUBLIC",
        session_parameters={
            "CLIENT_TELEMETRY_ENABLED": False,
        },
    )

    return conn

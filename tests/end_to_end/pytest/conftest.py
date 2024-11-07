#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

import os

import pytest
import featureform as ff
from dotenv import load_dotenv


@pytest.fixture(scope="session", autouse=True)
def before_tests():
    # find the path to the .env file
    # it will be in the root of the project which is three levels up
    repo_directory = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    )
    env_path = os.path.join(repo_directory, ".env")

    print("Loading environment variables from:", env_path)
    # Load environment variables from .env file
    load_dotenv(env_path)


@pytest.fixture(scope="session")
def num_records_limit():
    return 10


@pytest.fixture(scope="function")
def ff_client():
    client = ff.Client(host="localhost:7878", insecure=True)

    return client


@pytest.fixture(scope="session")
def client():
    client = ff.Client(host="localhost:7878", insecure=True)

    return client


@pytest.fixture(scope="session")
def redis_fixture(client):
    redis = ff.register_redis(
        name="redis-fixture",
        host=os.getenv("REDIS_HOST", "localhost"),  # The internal dns name for redis
        port=6379,
        description="A Redis deployment we created for the Featureform quickstart",
    )

    client.apply(asynchronous=False, verbose=True)

    return redis


@pytest.fixture(scope="module")
def postgres_fixture(client):
    postgres = ff.register_postgres(
        name="postgres-fixture",
        host=os.getenv(
            "POSTGRES_HOST", "localhost"
        ),  # The internal dns name for postgres
        port="5432",
        user="postgres",
        password="password",
        database="postgres",
        description="A Postgres deployment we created for the Featureform quickstart",
    )

    client.apply(asynchronous=False, verbose=True)

    return postgres


@pytest.fixture(scope="module")
def postgres_transactions_dataset(client, postgres_fixture):
    postgres = postgres_fixture
    transactions = postgres.register_table(
        name="transaction",
        variant=f"default",
        table="transactions",  # This is the table's name in Postgres
    )

    client.apply(asynchronous=False, verbose=True)

    return transactions

import json
import sys
import pytest
import os

sys.path.insert(0, 'client/src/')
from featureform.resources import BigQueryConfig, FirestoreConfig


@pytest.fixture
def connection_configs():
    return json.load(open("provider/connection/connection_configs.json"))


@pytest.fixture
def bigquery_config(connection_configs):
    return connection_configs["BigQuery"]


def test_bigquery(bigquery_config):
    conf = BigQueryConfig(
        project_id=bigquery_config["ProjectID"],
        dataset_id=bigquery_config["DatasetID"],
        credentials_path="provider/connection/gcp_test_credentials.json"
    )
    serialized_config = conf.serialize()
    assert json.loads(serialized_config) == bigquery_config


@pytest.fixture
def firestore_config(connection_configs):
    return connection_configs["Firestore"]


def test_firestore(firestore_config):
    conf = FirestoreConfig(
        project_id=firestore_config["ProjectID"],
        collection=firestore_config["Collection"],
        credentials_path="provider/connection/gcp_test_credentials.json"
    )
    serialized_config = conf.serialize()
    assert json.loads(serialized_config) == firestore_config

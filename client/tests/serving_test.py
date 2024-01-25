import csv
import os
import shutil
import stat
import sys
import time
from tempfile import NamedTemporaryFile
from unittest import TestCase
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest

from featureform.local_utils import feature_df_with_entity, label_df_from_csv

sys.path.insert(0, "client/src/")
from featureform import ResourceClient, ServingClient
import serving_cases as cases
import featureform as ff
from featureform.serving import LocalClientImpl, check_feature_type, Row, Dataset


@pytest.mark.parametrize(
    "test_input,expected",
    [
        ([("name", "variant")], [("name", "variant")]),
        (["name"], [("name", "default")]),
        (["name1", "name2"], [("name1", "default"), ("name2", "default")]),
        (["name1", ("name2", "variant")], [("name1", "default"), ("name2", "variant")]),
    ],
)
def test_check_feature_type(test_input, expected):
    assert expected == check_feature_type(test_input)


class TestIndividualFeatures(TestCase):
    def test_process_feature_no_ts(self):
        for name, case in cases.features_no_ts.items():
            with self.subTest(name):
                print("TEST: ", name)
                file_name = create_temp_file(case)
                client = ServingClient(local=True)
                local_client = LocalClientImpl()
                dataframe_mapping = feature_df_with_entity(file_name, "entity_id", case)
                expected = pd.DataFrame(case["expected"])
                actual = dataframe_mapping
                expected = expected.values.tolist()
                actual = actual.values.tolist()
                local_client.db.close()
                client.impl.db.close()
                assert all(
                    elem in expected for elem in actual
                ), "Expected: {} Got: {}".format(expected, actual)

    def test_process_feature_with_ts(self):
        for name, case in cases.features_with_ts.items():
            with self.subTest(msg=name):
                print("TEST: ", name)
                file_name = create_temp_file(case)
                client = ServingClient(local=True)
                local_client = LocalClientImpl()
                dataframe_mapping = feature_df_with_entity(file_name, "entity_id", case)
                expected = pd.DataFrame(case["expected"])
                actual = dataframe_mapping
                expected = expected.values.tolist()
                actual = actual.values.tolist()
                local_client.db.close()
                client.impl.db.close()
                assert all(
                    elem in expected for elem in actual
                ), "Expected: {} Got: {}".format(expected, actual)

    def test_invalid_entity_col(self):
        case = cases.feature_invalid_entity
        file_name = create_temp_file(case)
        client = ServingClient(local=True)
        local_client = LocalClientImpl()
        with pytest.raises(KeyError) as err:
            feature_df_with_entity(file_name, "entity_id", case)
        local_client.db.close()
        client.impl.db.close()
        assert "column does not exist" in str(err.value)

    def test_invalid_value_col(self):
        case = cases.feature_invalid_value
        file_name = create_temp_file(case)
        client = ServingClient(local=True)
        local_client = LocalClientImpl()
        with pytest.raises(KeyError) as err:
            feature_df_with_entity(file_name, "entity_id", case)
        local_client.db.close()
        client.impl.db.close()
        assert "column does not exist" in str(err.value)

    def test_invalid_ts_col(self):
        case = cases.feature_invalid_ts
        file_name = create_temp_file(case)
        client = ServingClient(local=True)
        local_client = LocalClientImpl()
        with pytest.raises(KeyError) as err:
            feature_df_with_entity(file_name, "entity_id", case)
        local_client.db.close()
        client.impl.db.close()
        assert "column does not exist" in str(err.value)
        retry_delete()


class TestIndividualLabels(TestCase):
    def test_individual_labels(self):
        for name, case in cases.labels.items():
            with self.subTest(name):
                print("TEST: ", name)
                file_name = create_temp_file(case)
                actual = label_df_from_csv(case, file_name)
                expected = pd.DataFrame(case["expected"]).set_index(case["entity_name"])
                pd.testing.assert_frame_equal(actual, expected)

    def test_invalid_entity(self):
        case = {
            "columns": ["entity", "value", "ts"],
            "values": [],
            "entity_name": "entity",
            "source_entity": "name_dne",
            "source_value": "value",
            "source_timestamp": "ts",
        }
        file_name = create_temp_file(case)
        with pytest.raises(KeyError) as err:
            label_df_from_csv(case, file_name)
        assert "column does not exist" in str(err.value)

    def test_invalid_value(self):
        case = {
            "columns": ["entity", "value", "ts"],
            "values": [],
            "source_entity": "entity",
            "source_value": "value_dne",
            "source_timestamp": "ts",
        }
        file_name = create_temp_file(case)
        with pytest.raises(KeyError) as err:
            label_df_from_csv(case, file_name)
        assert "column does not exist" in str(err.value)

    def test_invalid_ts(self):
        case = {
            "columns": ["entity", "value", "ts"],
            "values": [],
            "source_entity": "entity",
            "source_value": "value",
            "source_timestamp": "ts_dne",
        }
        file_name = create_temp_file(case)
        with pytest.raises(KeyError) as err:
            label_df_from_csv(case, file_name)
        assert "column does not exist" in str(err.value)

    @pytest.fixture(autouse=True)
    def run_before_and_after_tests(tmpdir):
        """Fixture to execute asserts before and after a test is run"""
        # Remove any lingering Databases
        try:
            shutil.rmtree(".featureform", onerror=del_rw)
        except:
            print("File Already Removed")
        yield
        try:
            shutil.rmtree(".featureform", onerror=del_rw)
        except:
            print("File Already Removed")


@pytest.fixture
def proto_row():
    class ProtoRow:
        def __init__(self):
            self.features = [1, 2, 3]
            self.label = 4

        def to_numpy(self):
            row = np.array(self.features)
            row = np.append(row, self.label)
            return row

    return ProtoRow()


def test_row_to_numpy(proto_row):
    def side_effect(value):
        if value in proto_row.features:
            return value
        else:
            return proto_row.label

    ff.serving.parse_proto_value = MagicMock(side_effect=side_effect)

    row = Row(proto_row)
    row_np = row.to_numpy()
    proto_row_np = proto_row.to_numpy()

    assert np.array_equal(row_np, proto_row_np)


def replace_nans(row):
    """
    Replaces NaNs in a list with the string 'NaN'. Dealing with NaN's can be a pain in Python so this is a
    helper function to make it easier to test.
    """
    result = []
    for r in row:
        if isinstance(r, float) and np.isnan(r):
            result.append("NaN")
        else:
            result.append(r)
    return result


def clear_and_reset():
    ff.clear_state()
    shutil.rmtree(".featureform", onerror=del_rw)


def del_rw(action, name, exc):
    os.chmod(name, stat.S_IWRITE)
    os.remove(name)


def create_temp_file(test_values):
    file = NamedTemporaryFile(delete=False, suffix=".csv")
    with open(file.name, "w") as csvfile:
        writer = csv.writer(csvfile, delimiter=",", quotechar="|")
        writer.writerow(test_values["columns"])
        for row in test_values["values"]:
            writer.writerow(row)
        csvfile.close()

    return file.name


def e2e_features(
    file, entity_name, entity_loc, name_variant_type, value_cols, entities, ts_col
):
    import uuid

    local = ff.local
    transactions = ff.local.register_file(
        name="transactions",
        variant=str(uuid.uuid4())[:8],
        description="A dataset of fraudulent transactions",
        path=file,
    )
    entity = ff.register_entity(entity_name)
    for i, (name, variant, type) in enumerate(name_variant_type):
        transactions.register_resources(
            entity=entity,
            entity_column=entity_loc,
            inference_store=local,
            features=[
                {
                    "name": name,
                    "variant": variant,
                    "column": value_cols[i],
                    "type": type,
                },
            ],
            timestamp_column=ts_col,
        )
    ResourceClient(local=True).apply()
    client = ServingClient(local=True)
    results = []
    name_variant = [(name, variant) for name, variant, type in name_variant_type]
    for entity in entities:
        results.append(client.features(name_variant, entity))

    return results


def retry_delete():
    for i in range(0, 100):
        try:
            shutil.rmtree(".featureform", onerror=del_rw)
            print("Table Deleted")
            break
        except Exception as e:
            print(f"Could not delete. Retrying...", e)
            time.sleep(1)


def test_read_directory():
    from pandas.testing import assert_frame_equal

    local = LocalClientImpl()
    df = local.read_directory("client/tests/test_files/input_files/readable_directory")
    expected = pd.DataFrame(
        data={"filename": ["c.txt", "b.txt", "a.txt"], "body": ["Sup", "Hi", "Hello"]}
    )
    expected = expected.sort_values(by=expected.columns.tolist()).reset_index(drop=True)
    df = df.sort_values(by=df.columns.tolist()).reset_index(drop=True)
    assert_frame_equal(expected, df)


@pytest.mark.parametrize(
    "location, expected_location",
    [
        ("s3://bucket/path/to/file.csv", "s3a://bucket/path/to/file.csv"),
        ("s3a://bucket/path/to/file.csv", "s3a://bucket/path/to/file.csv"),
        (
            "s3://bucket/path/to/directory/part-0000.parquet",
            "s3a://bucket/path/to/directory",
        ),
        ("s3://bucket/path/to/directory", "s3a://bucket/path/to/directory"),
    ],
)
def test_sanitize_location(location, expected_location):
    dataset = Dataset("")
    assert dataset._sanitize_location(location) == expected_location


@pytest.mark.parametrize(
    "location,format",
    [
        ("client/tests/test_files/input_files/transactions.csv", "csv"),
        ("client/tests/test_files/input_files/transactions.parquet", "parquet"),
    ],
)
def test_get_spark_dataframe(location, format, spark_session):
    expected_df = (
        spark_session.read.option("header", "true").format(format).load(location)
    )
    dataset = Dataset("")
    actual_df = dataset._get_spark_dataframe(spark_session, format, location)
    assert actual_df.collect() == expected_df.collect()

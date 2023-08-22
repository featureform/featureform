import os.path
import shutil
import stat

import featureform as ff
import pandas as pd
import pytest
from dataclasses import dataclass
from featureform import local, ServingClient

real_path = os.path.realpath(__file__)
dir_path = os.path.dirname(real_path)
SOURCE_FILE = f"{dir_path}/test_files/input_files/transactions.csv"


@dataclass
class SetupFixture:
    transactions_file: str
    serving_client: ServingClient


@pytest.fixture(scope="function")
def setup(tmp_path_factory):
    variant = "cache_test"

    temp_dir = tmp_path_factory.mktemp("test_inputs")
    temp_transactions = temp_dir / "transactions.csv"
    shutil.copy(SOURCE_FILE, temp_transactions)
    transactions = local.register_file(
        name="transactions",
        variant=variant,
        path=str(temp_transactions),
    )

    @local.df_transformation(variant=variant, inputs=[("transactions", variant)])
    def average_user_transaction(transactions):
        """the average transaction amount for a user"""
        return transactions.groupby("CustomerID")["TransactionAmount"].mean()

    user = ff.register_entity("user")

    # Register a column from our transformation as a feature
    average_user_transaction.register_resources(
        entity=user,
        entity_column="CustomerID",
        inference_store=local,
        features=[
            {
                "name": "avg_transactions",
                "variant": variant,
                "column": "TransactionAmount",
                "type": "float32",
            },
        ],
    )

    # Register label from our base Transactions table
    transactions.register_resources(
        entity=user,
        entity_column="CustomerID",
        timestamp_column="Timestamp",
        labels=[
            {
                "name": "fraudulent",
                "variant": variant,
                "column": "IsFraud",
                "type": "bool",
            },
        ],
    )

    ff.register_training_set(
        "fraud_training",
        variant,
        label=("fraudulent", variant),
        features=[("avg_transactions", variant)],
    )

    with ff.Client(local=True) as client:
        client.apply()
        client.training_set("fraud_training", variant)  # this will create the cache

        yield SetupFixture(
            transactions_file=str(temp_transactions), serving_client=client
        )


@pytest.fixture(autouse=True)
def cleanup():
    yield
    shutil.rmtree(".featureform", onerror=del_rw)


def del_rw(action, name, exc):
    if os.path.exists(name):
        os.chmod(name, stat.S_IWRITE)
        os.remove(name)


class TestLocalCache:
    def test_cache_files_are_created(self, setup):
        """
        Sets up local mode with transactions.csv. Registers a transformation, a feature, and a label.
        Ensures all cached files are created
        """

        cache_files = os.listdir(".featureform/cache")

        expected_cache_files = [
            "transformation__average_user_transaction__cache_test.pkl",
            "feature__avg_transactions__cache_test.pkl",
            "label__fraudulent__cache_test.pkl",
            "training_set__fraud_training__cache_test.pkl",
        ]

        assert os.path.exists(".featureform/cache")

        for expected_file in expected_cache_files:
            assert (
                expected_file in cache_files
            ), f"{expected_file} not found in cache_files"

    def test_label_cached_file_is_read_from(self, setup):
        fixture = setup

        # make a change to the cached file
        label_df = pd.read_pickle(
            ".featureform/cache/label__fraudulent__cache_test.pkl"
        )
        label_df["label"] = None
        label_df.to_pickle(".featureform/cache/label__fraudulent__cache_test.pkl")

        label = fixture.serving_client.impl.db.get_label_variant(
            "fraudulent", "cache_test"
        )
        label_df = fixture.serving_client.impl.get_label_dataframe(label)

        # ensure all label values are null
        assert label_df["label"].isnull().all()

    def test_transformation_cached_file_is_read_from(self, setup):
        fixture = setup

        # make a change to the cached file
        transformation_df = pd.read_pickle(
            ".featureform/cache/transformation__average_user_transaction__cache_test.pkl"
        )
        transformation_df["TransactionAmount"] = 0
        transformation_df.to_pickle(
            ".featureform/cache/transformation__average_user_transaction__cache_test.pkl"
        )

        transformation_df = fixture.serving_client.impl.process_transformation(
            "average_user_transaction", "cache_test"
        )

        # ensure all values are 0
        assert transformation_df["TransactionAmount"].all() == 0

    def test_training_set_cached_file_is_read_from(self, setup):
        fixture = setup

        # make a change to the cached file
        training_set_df = pd.read_pickle(
            ".featureform/cache/training_set__fraud_training__cache_test.pkl"
        )
        training_set_df["fraudulent"] = None
        training_set_df.to_pickle(
            ".featureform/cache/training_set__fraud_training__cache_test.pkl"
        )

        training_set = fixture.serving_client.training_set(
            "fraud_training", "cache_test"
        )
        training_set_df = training_set.pandas()

        assert training_set_df["fraudulent"].isnull().all()

    def test_label_cached_files_are_reset(self, setup):
        """
        Ensures all cached files are reset when the files are modified.
        """
        fixture = setup

        # modify the file. This should reset the cache
        transactions = pd.read_csv(fixture.transactions_file)
        transactions["IsFraud"] = None
        transactions.to_csv(fixture.transactions_file, index=False)

        label = fixture.serving_client.impl.db.get_label_variant(
            "fraudulent", "cache_test"
        )
        label_df = fixture.serving_client.impl.get_label_dataframe(label)

        # ensure all values are null
        assert label_df["label"].isnull().all()

    def test_training_set_cached_files_are_reset(self, setup):
        """
        Ensures all cached files are reset when the files are modified.
        """
        fixture = setup

        # modify the file. This should reset the cache
        transactions = pd.read_csv(fixture.transactions_file)
        transactions["IsFraud"] = None
        transactions.to_csv(fixture.transactions_file, index=False)

        training_set = fixture.serving_client.training_set(
            "fraud_training", "cache_test"
        )
        training_set_df = training_set.pandas()

        assert training_set_df["label"].isnull().all()

    def test_transformation_cached_files_are_reset(self, setup):
        """
        Ensures all cached files are reset when the files are modified.
        """
        fixture = setup

        # modify the file. This should reset the cache
        transactions = pd.read_csv(fixture.transactions_file)
        transactions["TransactionAmount"] = 0
        transactions.to_csv(fixture.transactions_file, index=False)

        transformation_df = fixture.serving_client.impl.process_transformation(
            "average_user_transaction", "cache_test"
        )

        # ensure all values are 0
        assert transformation_df.sum() == 0

    def test_feature_cached_files_are_reset(self, setup):
        """
        Ensures all cached files are reset when the files are modified.
        """
        fixture = setup

        # modify the file. This should reset the cache
        transactions = pd.read_csv(fixture.transactions_file)
        transactions["TransactionAmount"] = 0
        transactions.to_csv(fixture.transactions_file, index=False)

        feature = fixture.serving_client.impl.db.get_feature_variant(
            "avg_transactions", "cache_test"
        )
        feature_df = fixture.serving_client.impl.get_feature_dataframe(feature)

        # ensure all values are 0
        assert feature_df["avg_transactions.cache_test"].all() == 0

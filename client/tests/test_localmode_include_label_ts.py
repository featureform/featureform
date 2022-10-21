import featureform as ff
from featureform import local
import pandas as pd
import pytest


def setup():
    transactions = local.register_file(
        name="transactions",
        variant="quickstart",
        description="A dataset of fraudulent transactions",
        path="transactions.csv"
    )

    @local.df_transformation(variant="quickstart",
                                inputs=[("transactions", "quickstart")])
    def average_user_transaction(transactions):
        """the average transaction amount for a user """
        return transactions.groupby("CustomerID")["TransactionAmount"].mean()

    @local.sql_transformation(variant="quickstart")
    def sql_average_user_transaction():
        """the average transaction amount for a user """
        return "SELECT CustomerID as user_id, avg(TransactionAmount) " \
            "as avg_transaction_amt from {{transactions.kaggle}} GROUP BY user_id"

    user = ff.register_entity("user")

    # Register a column from our transformation as a feature
    average_user_transaction.register_resources(
        entity=user,
        entity_column="CustomerID",
        inference_store=local,
        features=[
            {"name": "avg_transactions", "variant": "quickstart", "column": "TransactionAmount", "type": "float32"},
        ],
    )

    sql_average_user_transaction.register_resources(
        entity=user,
        entity_column="CustomerID",
        inference_store=local,
        features=[
            {"name": "avg_transactions", "variant": "sql", "column": "TransactionAmount", "type": "float32"},
        ],
    )

    # Register label from our base Transactions table
    transactions.register_resources(
        entity=user,
        entity_column="CustomerID",
        timestamp_column="Timestamp",
        labels=[
            {"name": "fraudulent", "variant": "quickstart", "column": "IsFraud", "type": "bool"},
        ],
    )

    # Register label from our base Transactions table
    transactions.register_resources(
        entity=user,
        entity_column="CustomerID",
        labels=[
            {"name": "fraudulent", "variant": "quickstart-no-ts", "column": "IsFraud", "type": "bool"},
        ],
    )

    ff.register_training_set(
        "fraud_training", "quickstart",
        label=("fraudulent", "quickstart"),
        features=[("avg_transactions", "quickstart")],
    )

    ff.register_training_set(
        "fraud_training", "quickstart-no-ts",
        label=("fraudulent", "quickstart-no-ts"),
        features=[("avg_transactions", "quickstart")],
    )

    resource_client = ff.ResourceClient(local=True)
    resource_client.apply()


@pytest.mark.parametrize(
    "training_set_variant, include_label_timestamp, expected_in_keys",
    [
        ("quickstart", True, True),
        ("quickstart", False, False),
        ("quickstart-no-ts", True, False),
        ("quickstart-no-ts", False, False),
    ]
)
def test_include_label_timestamp(training_set_variant, include_label_timestamp, expected_in_keys):

    serving_client = ff.ServingClient(local=True)
    dataset = serving_client.training_set('fraud_training', training_set_variant, include_label_timestamp=include_label_timestamp)
    dataset_pandas = dataset.pandas()
    
    result = "label_timestamp" in dataset_pandas.keys()

    assert result == expected_in_keys

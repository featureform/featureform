import featureform as ff
import pytest
from featureform.resources import ResourceRedefinedError

class TestResourcesRedefined:
    def test_duplicate_resources(self):
        ff.register_user("featureformer").make_default_owner()

        local = ff.register_local()
        local = ff.register_local()

        transactions = local.register_file(
            name="transactions",
            variant="quickstart",
            description="A dataset of fraudulent transactions",
            path="transactions.csv"
        )

        transactions = local.register_file(
            name="transactions",
            variant="quickstart",
            description="A dataset of fraudulent transactions",
            path="transactions.csv"
        )

        user = ff.register_entity("user")
        user = ff.register_entity("user")

        @local.df_transformation(variant="quickstart",
                                 inputs=[("transactions", "quickstart")])
        def average_user_transaction(transactions):
            """the average transaction amount for a user """
            return transactions.groupby("CustomerID")["TransactionAmount"].mean()

        # Register a column from our transformation as a feature
        average_user_transaction.register_resources(
            entity=user,
            entity_column="CustomerID",
            inference_store=local,
            features=[
                {"name": "avg_transactions", "variant": "quickstart", "column": "TransactionAmount", "type": "float32"},
            ],
        )
        average_user_transaction.register_resources(
            entity=user,
            entity_column="CustomerID",
            inference_store=local,
            features=[
                {"name": "avg_transactions", "variant": "quickstart", "column": "TransactionAmount", "type": "float32"},
            ],
        )

        # Register label from our base Transactions table
        transactions.register_resources(
            entity=user,
            entity_column="CustomerID",
            labels=[
                {"name": "fraudulent", "variant": "quickstart", "column": "IsFraud", "type": "bool"},
            ],
        )
        transactions.register_resources(
            entity=user,
            entity_column="CustomerID",
            labels=[
                {"name": "fraudulent", "variant": "quickstart", "column": "IsFraud", "type": "bool"},
            ],
        )

        ff.register_training_set(
            "fraud_training", "quickstart",
            label=("fraudulent", "quickstart"),
            features=[("avg_transactions", "quickstart")],
        )

        ff.register_training_set(
            "fraud_training", "quickstart",
            label=("fraudulent", "quickstart"),
            features=[("avg_transactions", "quickstart")],
        )

        client = ff.Client(local=True)
        client.apply()


    def test_diff_resources_same_name_variant(self):
        client = ff.Client(local=True)
        ff.register_user("featureformer").make_default_owner()
        local = ff.register_local()

        transactions = local.register_file(
            name="transactions",
            variant="quickstart",
            description="A dataset of fraudulent transactions",
            path="transactions.csv"
        )

        user = ff.register_entity("user")

        client.apply()

        @local.df_transformation(variant="quickstart",
                                 inputs=[("transactions", "quickstart")])
        def average_user_transaction(transactions):
            """the average transaction amount for a user """
            return transactions.groupby("CustomerID")["TransactionAmount"].mean()

        # Register a column from our transformation as a feature
        average_user_transaction.register_resources(
            entity=user,
            entity_column="CustomerID",
            inference_store=local,
            features=[
                {"name": "avg_transactions", "variant": "quickstart", "column": "TransactionAmount", "type": "float32"},
            ],
        )
        average_user_transaction.register_resources(
            entity=user,
            entity_column="CustomerID",
            inference_store=local,
            features=[
                {"name": "avg_transactions", "variant": "quickstart", "column": "TransactionAmount", "type": "float64"},
            ],
        )

        with pytest.raises(ResourceRedefinedError):
            client.apply()

        # Register label from our base Transactions table
        transactions.register_resources(
            entity=user,
            entity_column="CustomerID",
            labels=[
                {"name": "fraudulent", "variant": "quickstart", "column": "IsFraud", "type": "bool"},
            ],
        )
        transactions.register_resources(
            entity=user,
            entity_column="CustomerID",
            labels=[
                {"name": "fraudulent", "variant": "quickstart", "column": "IsFraud", "type": "boolean"},
            ],
        )
        with pytest.raises(ResourceRedefinedError):
            client.apply()

        ff.register_training_set(
            "fraud_training", "quickstart",
            label=("fraudulent", "quickstart"),
            features=[("avg_transactions", "quickstart")],
        )
        ff.register_training_set(
            "fraud_training", "quickstart",
            label=("fraudulent", "qwickstart"),
            features=[("avg_tranzactions", "quickstart")],
        )

        with pytest.raises(ResourceRedefinedError):
            client.apply()

    def test_cleanup(tmpdir):
        try:
            client = ff.ServingClient(local=True)
            client.sqldb.close()
            shutil.rmtree('.featureform', onerror=del_rw)
        except:
            print("File Already Removed")

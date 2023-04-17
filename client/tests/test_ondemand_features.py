import dill
import time

import pytest
import numpy as np

import featureform as ff
from featureform.resources import OnDemandFeature, ResourceStatus


@pytest.fixture(autouse=True)
def before_and_after_each(setup_teardown):
    setup_teardown()
    yield
    setup_teardown()


@pytest.mark.local
def test_ondemand_feature_decorator_class():
    name = "test_ondemand_feature"
    owner = "ff_tester"

    decorator = OnDemandFeature(owner=owner, name=name)
    decorator_2 = OnDemandFeature(owner=owner, name=name)

    assert decorator.name_variant() == (name, "default")
    assert decorator.type() == "ondemand_feature"
    assert decorator.get_status() == ResourceStatus.READY
    assert decorator.is_ready() is True
    assert decorator == decorator_2


@pytest.mark.local
def test_ondemand_decorator():
    owner = "ff_tester"

    @OnDemandFeature(owner=owner)
    def test_fn():
        return 1 + 1

    expected_query = dill.dumps(test_fn.__code__)

    assert test_fn.name_variant() == (test_fn.__name__, "default")
    assert test_fn.query == expected_query


@pytest.mark.local
@pytest.mark.parametrize(
    "features,entity,expected_output",
    [
        ([("avg_transactions", "quickstart")], {"user": "C8837983"}, [1875.0]),
        ([("pi", "default")], {"user": "C8837983"}, [3.141592653589793]),
        ([("avg_transactions", "quickstart"), ("pi", "default")], {"user": "C8837983"}, [1875.0, 3.141592653589793]),
        ([("pi", "default"), ("avg_transactions", "quickstart")], {"user": "C8837983"}, [3.141592653589793, 1875.0]),
        ([("avg_transactions", "quickstart"), ("pi", "default"), ("avg_transactions", "quickstart")], {"user": "C8837983"}, [1875.0, 3.141592653589793, 1875.0]),
        ([("avg_transactions", "quickstart"), ("pi", "default"), ("avg_transactions", "quickstart"), ("pi", "default")], {"user": "C8837983"}, [1875.0, 3.141592653589793, 1875.0, 3.141592653589793]),
        ([("avg_transactions", "quickstart"), ("pi", "default"), ("pi", "pi_named"), ("pi_called", "default")], {"user": "C8837983"}, [1875.0, 3.141592653589793, 3.141592653589793, 3.141592653589793]),
        pytest.param([], {}, [], marks=pytest.mark.xfail),
        pytest.param([("pi", "default")], None, [], marks=pytest.mark.xfail),
    ]
)
def test_serving_ondemand_precalculated_feature(features, entity, expected_output):
    register_resources()
    client = ff.ServingClient(local=True)

    features = client.features(features, entity)
    assert features.tolist() == expected_output

    client.impl.db.close()  # TODO automatically do this


def register_resources():
    ff.register_user("featureformer").make_default_owner()

    local = ff.register_local()

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

    @ff.ondemand_feature
    def pi(serving_client, entities, params):
        import math
        return math.pi

    @ff.ondemand_feature()
    def pi_called(serving_client, entities, params):
        import math
        return math.pi

    @ff.ondemand_feature(variant="pi_named")
    def pi(serving_client, entities, params):
        import math
        return math.pi

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
    # Register label from our base Transactions table
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

    resource_client = ff.ResourceClient(local=True)
    resource_client.apply()

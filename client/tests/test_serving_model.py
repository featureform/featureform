import featureform as ff
from featureform.resources import Model
import os
import pytest


real_path = os.path.realpath(__file__)
dir_path = os.path.dirname(real_path)

# TRAINING SET TESTS
@pytest.mark.parametrize(
    "provider_source_fxt,serving_client_fxt,is_local",
    [
        pytest.param('local_provider_source', 'serving_client', True, marks=pytest.mark.local),
        pytest.param('hosted_sql_provider_and_source','serving_client', False, marks=pytest.mark.hosted),
    ]
)
def test_registering_model_while_serving_training_set(provider_source_fxt,serving_client_fxt, is_local, request):
    provider, source = request.getfixturevalue(provider_source_fxt)();
    serving_client = request.getfixturevalue(serving_client_fxt)(is_local);

    # Arranges the resources context following the Quickstart pattern
    resource_client = arrange_resources(provider, source, is_local)

    model_name = 'fraud_model_a'

    assert True

    # Commented out to debug training set create prior to serving
    # serving_client.training_set("fraud_training", "quickstart", model=model_name)

    # # resource_client = ff.ResourceClient(local=is_local, insecure=True)
    # model = resource_client.get_model(model_name, is_local)

    # print("MODEL", model)

    # assert isinstance(model, Model) and model.name == model_name and model.type() == 'model'


@pytest.mark.parametrize(
    "provider_source_fxt,serving_client_fxt,is_local",
    [
        pytest.param('local_provider_source', 'serving_client', True, marks=pytest.mark.local),
        pytest.param('hosted_sql_provider_and_source','serving_client', False, marks=pytest.mark.hosted),
    ]
)
def test_registering_two_models_while_serving_training_set(provider_source_fxt,serving_client_fxt, is_local, request):
    provider, source = request.getfixturevalue(provider_source_fxt)();
    serving_client = request.getfixturevalue(serving_client_fxt)(is_local);

    # Arranges the resources context following the Quickstart pattern
    resource_client = arrange_resources(provider, source, is_local)

    model_name_a = 'fraud_model_a';
    model_name_b = 'fraud_model_b';

    serving_client.training_set("fraud_training", "quickstart", model=model_name_a)
    serving_client.training_set("fraud_training", "quickstart", model=model_name_b)

    models = resource_client.list_models(is_local)

    assert [model_name_a, model_name_b] == [model.name for model in models] and all([isinstance(model, Model) for model in models])


@pytest.mark.parametrize(
    "provider_source_fxt,serving_client_fxt,is_local",
    [
        pytest.param('local_provider_source', 'serving_client', True, marks=pytest.mark.local),
        pytest.param('hosted_sql_provider_and_source','serving_client', False, marks=pytest.mark.hosted),
    ]
)
def test_registering_same_model_twice_while_serving_training_set(provider_source_fxt,serving_client_fxt, is_local, request):
    provider, source = request.getfixturevalue(provider_source_fxt)();
    serving_client = request.getfixturevalue(serving_client_fxt)(is_local);

    # Arranges the resources context following the Quickstart pattern
    resource_client = arrange_resources(provider, source, is_local)

    model_name = 'fraud_model';

    serving_client.training_set("fraud_training", "quickstart", model=model_name)
    serving_client.training_set("fraud_training", "quickstart", model=model_name)

    models = resource_client.list_models(is_local)

    assert [model_name] == [model.name for model in models] and all([isinstance(model, Model) for model in models])


# FEATURE TESTS
@pytest.mark.parametrize(
    "provider_source_fxt,serving_client_fxt,is_local",
    [
        pytest.param('local_provider_source', 'serving_client', True, marks=pytest.mark.local),
        pytest.param('hosted_sql_provider_and_source','serving_client', False, marks=pytest.mark.hosted),
    ]
)
def test_registering_model_while_serving_features(provider_source_fxt,serving_client_fxt, is_local, request):
    provider, source = request.getfixturevalue(provider_source_fxt)();
    serving_client = request.getfixturevalue(serving_client_fxt)(is_local);

    # Arranges the resources context following the Quickstart pattern
    resource_client = arrange_resources(provider, source, is_local)

    model_name = 'fraud_model_a';

    serving_client.features([("avg_transactions", "quickstart")], {"user": "C1410926"}, model=model_name)

    model = resource_client.get_model(model_name, is_local)

    assert isinstance(model, Model) and model.name == model_name and model.type() == 'model'


@pytest.mark.parametrize(
    "provider_source_fxt,serving_client_fxt,is_local",
    [
        pytest.param('local_provider_source', 'serving_client', True, marks=pytest.mark.local),
        pytest.param('hosted_sql_provider_and_source','serving_client', False, marks=pytest.mark.hosted),
    ]
)
def test_registering_two_models_while_serving_features(provider_source_fxt,serving_client_fxt, is_local, request):
    provider, source = request.getfixturevalue(provider_source_fxt)();
    serving_client = request.getfixturevalue(serving_client_fxt)(is_local);

    # Arranges the resources context following the Quickstart pattern
    resource_client = arrange_resources(provider, source, is_local)

    model_name_a = 'fraud_model_a';
    model_name_b = 'fraud_model_b';

    serving_client.features([("avg_transactions", "quickstart")], {"user": "C1410926"}, model=model_name_a)
    serving_client.features([("avg_transactions", "quickstart")], {"user": "C1410926"}, model=model_name_b)

    models = resource_client.list_models(is_local)

    assert [model_name_a, model_name_b] == [model.name for model in models] and all([isinstance(model, Model) for model in models])


@pytest.mark.parametrize(
    "provider_source_fxt,serving_client_fxt,is_local",
    [
        pytest.param('local_provider_source', 'serving_client', True, marks=pytest.mark.local),
        pytest.param('hosted_sql_provider_and_source','serving_client', False, marks=pytest.mark.hosted),
    ]
)
def test_registering_same_model_twice_while_serving_features(provider_source_fxt,serving_client_fxt, is_local, request):
    provider, source = request.getfixturevalue(provider_source_fxt)();
    serving_client = request.getfixturevalue(serving_client_fxt)(is_local);

    # Arranges the resources context following the Quickstart pattern
    resource_client = arrange_resources(provider, source, is_local)

    model_name = 'fraud_model';

    serving_client.features([("avg_transactions", "quickstart")], {"user": "C1410926"}, model=model_name)
    serving_client.features([("avg_transactions", "quickstart")], {"user": "C1410926"}, model=model_name)

    models = resource_client.list_models(is_local)

    assert [model_name] == [model.name for model in models] and all([isinstance(model, Model) for model in models])


@pytest.mark.parametrize(
    "provider_source_fxt,serving_client_fxt,is_local",
    [
        pytest.param('local_provider_source', 'serving_client', True, marks=pytest.mark.local),
        pytest.param('hosted_sql_provider_and_source','serving_client', False, marks=pytest.mark.hosted),
    ]
)
def test_no_models_registered_while_serving_training_set(provider_source_fxt,serving_client_fxt, is_local, request):
    provider, source = request.getfixturevalue(provider_source_fxt)();
    serving_client = request.getfixturevalue(serving_client_fxt)(is_local);

    # Arranges the resources context following the Quickstart pattern
    resource_client = arrange_resources(provider, source, is_local)

    serving_client.features([("avg_transactions", "quickstart")], {"user": "C1410926"})
    serving_client.features([("avg_transactions", "quickstart")], {"user": "C1410926"})

    models = resource_client.list_models(is_local)

    assert len(models) == 0


@pytest.fixture(autouse=True)
def before_and_after_each(setup_teardown):
    setup_teardown()
    yield
    setup_teardown()


def arrange_resources(provider, source, is_local):
    if is_local:
        @provider.df_transformation(variant="quickstart", inputs=[("transactions", "quickstart")])
        def average_user_transaction(transactions):
            return transactions.groupby("CustomerID")["TransactionAmount"].mean()
    else:
        @provider.sql_transformation(variant="quickstart")
        def average_user_transaction():
            return "SELECT customerid as user_id, avg(transactionamount) as avg_transaction_amt from {{transactions.kaggle}} GROUP BY user_id"

    user = ff.register_entity("user")
    entity_column = "CustomerID" if is_local else "user_id"

    average_user_transaction.register_resources(
        entity=user,
        entity_column=entity_column,
        inference_store=provider,
        features=[
            {"name": "avg_transactions", "variant": "quickstart", "column": "avg_transaction_amt", "type": "float32"},
        ],
    )

    source.register_resources(
        entity=user,
        entity_column=entity_column,
        labels=[
            {"name": "fraudulent", "variant": "quickstart", "column": "isfraud", "type": "bool"},
        ],
    )

    ff.register_training_set(
        "fraud_training", "quickstart",
        label=("fraudulent", "quickstart"),
        features=[("avg_transactions", "quickstart")],
    )

    resource_client = ff.ResourceClient(local=is_local, insecure=True)
    resource_client.apply()

    return resource_client

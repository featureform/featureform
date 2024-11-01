#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

import time
import featureform as ff
from featureform.resources import Model
import pytest


@pytest.mark.parametrize(
    "provider_source_fxt,serving_client_fxt,is_local,is_insecure",
    [
        pytest.param(
            "hosted_sql_provider_and_source",
            "serving_client",
            False,
            False,
            marks=pytest.mark.hosted,
        ),
        pytest.param(
            "hosted_sql_provider_and_source",
            "serving_client",
            False,
            True,
            marks=pytest.mark.docker,
        ),
    ],
)
def test_no_models_registered_while_serving_training_set(
    provider_source_fxt, serving_client_fxt, is_local, is_insecure, request
):
    custom_marks = [
        mark.name for mark in request.node.own_markers if mark.name != "parametrize"
    ]
    provider, source, inference_store = request.getfixturevalue(provider_source_fxt)(
        custom_marks
    )
    serving_client = request.getfixturevalue(serving_client_fxt)(is_local, is_insecure)

    # Arranges the resources context following the Quickstart pattern
    resource_client = arrange_resources(
        provider, source, inference_store, is_local, is_insecure
    )

    fts = serving_client.features(
        [("avg_transactions", "quickstart")], {"user": "C1410926"}
    )

    models = resource_client.list_models(is_local)

    assert len(models) == 0

    # TODO: Shouldn't have to do this
    if is_local:
        serving_client.impl.db.close()


@pytest.mark.parametrize(
    "provider_source_fxt,serving_client_fxt,is_local,is_insecure",
    [
        pytest.param(
            "hosted_sql_provider_and_source",
            "serving_client",
            False,
            False,
            marks=pytest.mark.hosted,
        ),
        pytest.param(
            "hosted_sql_provider_and_source",
            "serving_client",
            False,
            True,
            marks=pytest.mark.docker,
        ),
    ],
)
def test_registering_model_while_serving_training_set(
    provider_source_fxt, serving_client_fxt, is_local, is_insecure, request
):
    custom_marks = [
        mark.name for mark in request.node.own_markers if mark.name != "parametrize"
    ]
    provider, source, inference_store = request.getfixturevalue(provider_source_fxt)(
        custom_marks
    )
    serving_client = request.getfixturevalue(serving_client_fxt)(is_local, is_insecure)

    # Arranges the resources context following the Quickstart pattern
    resource_client = arrange_resources(
        provider, source, inference_store, is_local, is_insecure
    )

    model_name_a = "fraud_model_a"

    ts = serving_client.training_set("fraud_training", "quickstart", model=model_name_a)
    next(ts)

    model = resource_client.get_model(model_name_a, is_local)

    assert (
        isinstance(model, Model)
        and model.name == model_name_a
        and model.get_resource_type() == ff.ResourceType.MODEL
    )

    if is_local:
        serving_client.impl.db.close()


@pytest.mark.parametrize(
    "provider_source_fxt,serving_client_fxt,is_local,is_insecure",
    [
        pytest.param(
            "hosted_sql_provider_and_source",
            "serving_client",
            False,
            False,
            marks=pytest.mark.hosted,
        ),
        pytest.param(
            "hosted_sql_provider_and_source",
            "serving_client",
            False,
            True,
            marks=pytest.mark.docker,
        ),
    ],
)
def test_registering_two_models_while_serving_training_set(
    provider_source_fxt, serving_client_fxt, is_local, is_insecure, request
):
    custom_marks = [
        mark.name for mark in request.node.own_markers if mark.name != "parametrize"
    ]
    provider, source, inference_store = request.getfixturevalue(provider_source_fxt)(
        custom_marks
    )
    serving_client = request.getfixturevalue(serving_client_fxt)(is_local, is_insecure)

    # Arranges the resources context following the Quickstart pattern
    resource_client = arrange_resources(
        provider, source, inference_store, is_local, is_insecure
    )

    model_name_b = "fraud_model_b"
    model_name_c = "fraud_model_c"

    ts_1 = serving_client.training_set(
        "fraud_training", "quickstart", model=model_name_b
    )
    next(ts_1)
    ts_2 = serving_client.training_set(
        "fraud_training", "quickstart", model=model_name_c
    )
    next(ts_2)

    models = resource_client.list_models(is_local)
    models_names = [model.name for model in models]

    contains_expected_names = all(
        expected_name in models_names for expected_name in [model_name_b, model_name_c]
    )
    are_models_instances = all([isinstance(model, Model) for model in models])

    assert contains_expected_names and are_models_instances

    if is_local:
        serving_client.impl.db.close()


@pytest.mark.parametrize(
    "provider_source_fxt,serving_client_fxt,is_local,is_insecure",
    [
        pytest.param(
            "hosted_sql_provider_and_source",
            "serving_client",
            False,
            False,
            marks=pytest.mark.hosted,
        ),
        pytest.param(
            "hosted_sql_provider_and_source",
            "serving_client",
            False,
            True,
            marks=pytest.mark.docker,
        ),
    ],
)
def test_registering_same_model_twice_while_serving_training_set(
    provider_source_fxt, serving_client_fxt, is_local, is_insecure, request
):
    custom_marks = [
        mark.name for mark in request.node.own_markers if mark.name != "parametrize"
    ]
    provider, source, inference_store = request.getfixturevalue(provider_source_fxt)(
        custom_marks
    )
    serving_client = request.getfixturevalue(serving_client_fxt)(is_local, is_insecure)

    # Arranges the resources context following the Quickstart pattern
    resource_client = arrange_resources(
        provider, source, inference_store, is_local, is_insecure
    )

    model_name_d = "fraud_model_d"

    ts_1 = serving_client.training_set(
        "fraud_training", "quickstart", model=model_name_d
    )
    next(ts_1)
    ts_2 = serving_client.training_set(
        "fraud_training", "quickstart", model=model_name_d
    )
    next(ts_2)

    models = resource_client.list_models(is_local)
    expected = [model.name for model in models if model.name == model_name_d]

    assert (
        model_name_d in expected
        and len(expected) == 1
        and all([isinstance(model, Model) for model in models])
    )

    if is_local:
        serving_client.impl.db.close()


@pytest.mark.parametrize(
    "provider_source_fxt,serving_client_fxt,is_local,is_insecure",
    [
        pytest.param(
            "hosted_sql_provider_and_source",
            "serving_client",
            False,
            False,
            marks=pytest.mark.hosted,
        ),
        pytest.param(
            "hosted_sql_provider_and_source",
            "serving_client",
            False,
            True,
            marks=pytest.mark.docker,
        ),
    ],
)
def test_registering_model_while_serving_features(
    provider_source_fxt, serving_client_fxt, is_local, is_insecure, request
):
    custom_marks = [
        mark.name for mark in request.node.own_markers if mark.name != "parametrize"
    ]
    provider, source, inference_store = request.getfixturevalue(provider_source_fxt)(
        custom_marks
    )
    serving_client = request.getfixturevalue(serving_client_fxt)(is_local, is_insecure)

    # Arranges the resources context following the Quickstart pattern
    resource_client = arrange_resources(
        provider, source, inference_store, is_local, is_insecure
    )

    model_name_e = "fraud_model_e"

    fts = serving_client.features(
        [("avg_transactions", "quickstart")], {"user": "C1410926"}, model=model_name_e
    )

    model = resource_client.get_model(model_name_e, is_local)

    assert (
        isinstance(model, Model)
        and model.name == model_name_e
        and model.get_resource_type() == ff.ResourceType.MODEL
    )

    if is_local:
        serving_client.impl.db.close()


@pytest.mark.parametrize(
    "provider_source_fxt,serving_client_fxt,is_local,is_insecure",
    [
        pytest.param(
            "hosted_sql_provider_and_source",
            "serving_client",
            False,
            False,
            marks=pytest.mark.hosted,
        ),
        pytest.param(
            "hosted_sql_provider_and_source",
            "serving_client",
            False,
            True,
            marks=pytest.mark.docker,
        ),
    ],
)
def test_registering_two_models_while_serving_features(
    provider_source_fxt, serving_client_fxt, is_local, is_insecure, request
):
    custom_marks = [
        mark.name for mark in request.node.own_markers if mark.name != "parametrize"
    ]
    provider, source, inference_store = request.getfixturevalue(provider_source_fxt)(
        custom_marks
    )
    serving_client = request.getfixturevalue(serving_client_fxt)(is_local, is_insecure)

    # Arranges the resources context following the Quickstart pattern
    resource_client = arrange_resources(
        provider, source, inference_store, is_local, is_insecure
    )

    model_name_f = "fraud_model_f"
    model_name_g = "fraud_model_g"

    fts_1 = serving_client.features(
        [("avg_transactions", "quickstart")], {"user": "C1410926"}, model=model_name_f
    )
    fts_2 = serving_client.features(
        [("avg_transactions", "quickstart")], {"user": "C1410926"}, model=model_name_g
    )

    models = resource_client.list_models(is_local)
    models_names = [model.name for model in models]

    contains_expected_names = all(
        expected_name in models_names for expected_name in [model_name_f, model_name_g]
    )
    are_models_instances = all([isinstance(model, Model) for model in models])

    assert contains_expected_names and are_models_instances

    if is_local:
        serving_client.impl.db.close()


@pytest.mark.parametrize(
    "provider_source_fxt,serving_client_fxt,is_local,is_insecure",
    [
        pytest.param(
            "hosted_sql_provider_and_source",
            "serving_client",
            False,
            False,
            marks=pytest.mark.hosted,
        ),
        pytest.param(
            "hosted_sql_provider_and_source",
            "serving_client",
            False,
            True,
            marks=pytest.mark.docker,
        ),
    ],
)
def test_registering_same_model_twice_while_serving_features(
    provider_source_fxt, serving_client_fxt, is_local, is_insecure, request
):
    custom_marks = [
        mark.name for mark in request.node.own_markers if mark.name != "parametrize"
    ]
    provider, source, inference_store = request.getfixturevalue(provider_source_fxt)(
        custom_marks
    )
    serving_client = request.getfixturevalue(serving_client_fxt)(is_local, is_insecure)

    # Arranges the resources context following the Quickstart pattern
    resource_client = arrange_resources(
        provider, source, inference_store, is_local, is_insecure
    )

    model_name_h = "fraud_model_h"

    fts_1 = serving_client.features(
        [("avg_transactions", "quickstart")], {"user": "C1410926"}, model=model_name_h
    )
    fts_2 = serving_client.features(
        [("avg_transactions", "quickstart")], {"user": "C1410926"}, model=model_name_h
    )

    models = resource_client.list_models(is_local)
    expected = [model.name for model in models if model.name == model_name_h]

    assert (
        model_name_h in expected
        and len(expected) == 1
        and all([isinstance(model, Model) for model in models])
    )

    if is_local:
        serving_client.impl.db.close()


@pytest.fixture(autouse=True)
def before_and_after_each(setup_teardown):
    setup_teardown()
    yield
    setup_teardown()


def arrange_resources(provider, source, online_store, is_local, is_insecure):
    if is_local:

        @provider.df_transformation(
            variant="quickstart", inputs=[("transactions", "quickstart")]
        )
        def average_user_transaction(transactions):
            return transactions.groupby("CustomerID")["TransactionAmount"].mean()

    else:

        @provider.sql_transformation(variant="quickstart")
        def average_user_transaction():
            return "SELECT customerid as user_id, avg(transactionamount) as avg_transaction_amt from {{transactions.quickstart}} GROUP BY user_id"

    user = ff.register_entity("user")
    feature_column = "TransactionAmount" if is_local else "avg_transaction_amt"
    label_column = "IsFraud" if is_local else "isfraud"
    inference_store = provider if is_local else online_store

    average_user_transaction.register_resources(
        entity=user,
        entity_column="CustomerID" if is_local else "user_id",
        inference_store=inference_store,
        features=[
            {
                "name": "avg_transactions",
                "variant": "quickstart",
                "column": feature_column,
                "type": "float32",
            },
        ],
    )

    source.register_resources(
        entity=user,
        entity_column="CustomerID" if is_local else "customerid",
        labels=[
            {
                "name": "fraudulent",
                "variant": "quickstart",
                "column": label_column,
                "type": "bool",
            },
        ],
    )

    training_set_name = "fraud_training"
    training_set_variant = "quickstart"

    ff.register_training_set(
        training_set_name,
        training_set_variant,
        label=("fraudulent", "quickstart"),
        features=[("avg_transactions", "quickstart")],
    )

    resource_client = ff.ResourceClient(local=is_local, insecure=is_insecure)
    resource_client.apply(asynchronous=True)

    if not is_local:
        start = time.time()
        while True:
            time.sleep(3)
            ts = resource_client.get_training_set(
                training_set_name, training_set_variant
            )
            elapsed_wait = time.time() - start
            if (elapsed_wait >= 60) and ts.status != "READY":
                print(
                    f"Wait time for training set status exceeded; status is {ts.status}"
                )
                break
            elif ts.status == "READY":
                print(f"Training set is ready")
                break
            else:
                print(
                    f"Training set status is currently {ts.status} after {elapsed_wait} seconds ..."
                )
                continue

    return resource_client

#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

import pytest

import featureform as ff
from featureform import get_random_name
from featureform.register import Registrar, DFTransformationDecorator, ResourceRegistrar
from featureform.resources import Entity, FeatureVariant, LabelVariant


@pytest.mark.parametrize(
    "provider_source_fxt,is_local,is_insecure",
    [
        pytest.param(
            "hosted_sql_provider_and_source", False, False, marks=pytest.mark.hosted
        ),
        pytest.param(
            "hosted_sql_provider_and_source", False, True, marks=pytest.mark.docker
        ),
    ],
)
def test_class_api_syntax(provider_source_fxt, is_local, is_insecure, request):
    custom_marks = [
        mark.name for mark in request.node.own_markers if mark.name != "parametrize"
    ]
    provider, source, inference_store = request.getfixturevalue(provider_source_fxt)(
        custom_marks
    )

    arrange_resources(provider, source, inference_store, is_local, is_insecure)
    original_syntax_state = list(
        filter(
            lambda r: isinstance(r, (Entity, FeatureVariant, LabelVariant)),
            ff.state().sorted_list(),
        )
    )

    ff.clear_state()

    arrange_resources(
        provider, source, inference_store, is_local, is_insecure, is_class_api=True
    )
    class_syntax_state = list(
        filter(
            lambda r: isinstance(r, (Entity, FeatureVariant, LabelVariant)),
            ff.state().sorted_list(),
        )
    )

    # since the api's can have source objects rather than the name variants, we're going to get them in the same state
    _update_sources_to_name_variant(original_syntax_state)
    _update_sources_to_name_variant(class_syntax_state)

    assert original_syntax_state == class_syntax_state


def _update_sources_to_name_variant(resources):
    for r in resources:
        if isinstance(r, (FeatureVariant, LabelVariant)):
            if hasattr(r.source, "name_variant"):
                r.source = r.source.name_variant()


@pytest.mark.parametrize(
    "provider_source_fxt,is_local",
    [
        pytest.param("hosted_sql_provider_and_source", False, marks=pytest.mark.hosted),
        pytest.param("hosted_sql_provider_and_source", False, marks=pytest.mark.docker),
    ],
)
def test_variants_naming_consistency(provider_source_fxt, is_local, request):
    custom_marks = [
        mark.name for mark in request.node.own_markers if mark.name != "parametrize"
    ]
    _, source, _ = request.getfixturevalue(provider_source_fxt)(custom_marks)
    label_column = "IsFraud" if is_local else "isfraud"
    label_entity_column = "CustomerID" if is_local else "customerid"

    with pytest.raises(ValueError):
        ff.Variants(
            {
                "quickstart": ff.Label(
                    source[[label_entity_column, label_column]],
                    description="Whether a user's transaction is fraudulent.",
                    variant="quickstart_v2",
                    type=ff.Bool,
                ),
                "quickstart_v2": ff.Label(
                    source[[label_entity_column, label_column]],
                    description="Whether a user's transaction is fraudulent.",
                    variant="quickstart",
                    type=ff.Bool,
                ),
            }
        )


@pytest.mark.local
def test_subscriptable_transformation_decorator_method_call():
    variant = get_random_name()
    df_transformation_decorator = DFTransformationDecorator(
        registrar=Registrar(),
        provider="test_provider",
        owner="test_owner",
        tags=[],
        properties={},
        variant=variant,
    )

    def test_function():
        pass

    df_transformation_decorator(test_function)

    name_variant = df_transformation_decorator.name_variant()
    column_resource = df_transformation_decorator.register_resources(
        entity="user",
        entity_column="user_id",
        owner="test_owner",
        inference_store="test_store",
        features=[
            {
                "name": "avg_transactions",
                "variant": "quickstart",
                "column": "TransactionAmount",
                "type": "float32",
            }
        ],
    )

    assert name_variant == ("test_function", variant) and isinstance(
        column_resource, ResourceRegistrar
    )


@pytest.mark.parametrize(
    "provider_source_fxt,is_local",
    [
        pytest.param("hosted_sql_provider_and_source", False, marks=pytest.mark.hosted),
        pytest.param("hosted_sql_provider_and_source", False, marks=pytest.mark.docker),
    ],
)
def test_indexing_with_fewer_than_two_columns(provider_source_fxt, is_local, request):
    custom_marks = [
        mark.name for mark in request.node.own_markers if mark.name != "parametrize"
    ]
    _, source, _ = request.getfixturevalue(provider_source_fxt)(custom_marks)
    label_entity_column = "CustomerID" if is_local else "customerid"

    with pytest.raises(Exception, match="Expected 2 columns"):
        source[[label_entity_column]]


@pytest.mark.parametrize(
    "provider_source_fxt,is_local",
    [
        pytest.param("hosted_sql_provider_and_source", False, marks=pytest.mark.hosted),
        pytest.param("hosted_sql_provider_and_source", False, marks=pytest.mark.docker),
    ],
)
def test_indexing_with_more_than_three_columns(provider_source_fxt, is_local, request):
    custom_marks = [
        mark.name for mark in request.node.own_markers if mark.name != "parametrize"
    ]
    _, source, _ = request.getfixturevalue(provider_source_fxt)(custom_marks)
    label_entity_column = "CustomerID" if is_local else "customerid"
    label_timestamp_column = "Timestamp" if is_local else "timestamp"

    with pytest.raises(Exception, match="Found unrecognized columns"):
        source[
            [
                label_entity_column,
                label_entity_column,
                label_timestamp_column,
                "unknown_column",
            ]
        ]


@pytest.mark.parametrize(
    "provider_source_fxt,is_local",
    [
        pytest.param("hosted_sql_provider_and_source", False, marks=pytest.mark.hosted),
        pytest.param("hosted_sql_provider_and_source", False, marks=pytest.mark.docker),
    ],
)
def test_specifying_timestamp_column_twice(provider_source_fxt, is_local, request):
    custom_marks = [
        mark.name for mark in request.node.own_markers if mark.name != "parametrize"
    ]
    _, source, _ = request.getfixturevalue(provider_source_fxt)(custom_marks)
    label_column = "IsFraud" if is_local else "isfraud"
    label_entity_column = "CustomerID" if is_local else "customerid"
    timestamp_column = "Timestamp" if is_local else "timestamp"

    with pytest.raises(Exception, match="Timestamp column specified twice"):
        ff.Label(
            source[[label_entity_column, label_column, timestamp_column]],
            description="Whether a user's transaction is fraudulent.",
            variant="quickstart",
            type=ff.Bool,
            timestamp_column=timestamp_column,
        )


@pytest.fixture(autouse=True)
def before_and_after_each(setup_teardown):
    setup_teardown()
    yield
    setup_teardown()


def arrange_resources(
    provider, source, online_store, is_local, is_insecure, is_class_api=False
):
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

    inference_store = provider if is_local else online_store
    feature_column = "TransactionAmount" if is_local else "avg_transaction_amt"
    label_column = "IsFraud" if is_local else "isfraud"
    feature_entity_column = "CustomerID" if is_local else "user_id"
    label_entity_column = "CustomerID" if is_local else "customerid"
    timestamp_column = "Timestamp" if is_local else "timestamp"

    if is_class_api:

        @ff.entity
        class User:
            fraudulent = ff.Label(
                source[[label_entity_column, label_column, timestamp_column]],
                description="Whether a user's transaction is fraudulent.",
                variant="quickstart",
                type=ff.Bool,
            )
            avg_transactions = ff.Variants(
                {
                    "quickstart": ff.Feature(
                        average_user_transaction[
                            [feature_entity_column, feature_column]
                        ],
                        variant="quickstart",
                        type=ff.Float32,
                        inference_store=inference_store,
                        owner="test_user",
                        tags=["quickstart"],
                    ),
                    "quickstart_v2": ff.Feature(
                        average_user_transaction[
                            [feature_entity_column, feature_column]
                        ],
                        variant="quickstart_v2",
                        type=ff.Float32,
                        inference_store=inference_store,
                        owner="test_user",
                        properties={"version": "2.0"},
                    ),
                }
            )

    else:
        user = ff.register_entity("user")

        average_user_transaction.register_resources(
            entity=user,
            entity_column=feature_entity_column,
            inference_store=inference_store,
            features=[
                {
                    "name": "avg_transactions",
                    "variant": "quickstart",
                    "column": feature_column,
                    "type": "float32",
                    "tags": ["quickstart"],
                    "owner": "test_user",
                },
                {
                    "name": "avg_transactions",
                    "variant": "quickstart_v2",
                    "column": feature_column,
                    "type": "float32",
                    "properties": {"version": "2.0"},
                    "owner": "test_user",
                },
            ],
        )

        source.register_resources(
            entity=user,
            entity_column=label_entity_column,
            labels=[
                {
                    "name": "fraudulent",
                    "variant": "quickstart",
                    "column": label_column,
                    "type": "bool",
                    "description": "Whether a user's transaction is fraudulent.",
                },
            ],
            timestamp_column=timestamp_column,
        )

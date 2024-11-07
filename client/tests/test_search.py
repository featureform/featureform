#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

import time
import featureform as ff
from featureform.resources import Model
import os
import pytest


@pytest.fixture(autouse=True)
def before_and_after_each(setup_teardown):
    setup_teardown()
    yield
    setup_teardown()


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
def test_simple_search(provider_source_fxt, is_local, is_insecure, request):
    custom_marks = [
        mark.name for mark in request.node.own_markers if mark.name != "parametrize"
    ]
    provider, source, inference_store = request.getfixturevalue(provider_source_fxt)(
        custom_marks
    )

    # Arranges the resources context following the Quickstart pattern
    resource_client = arrange_resources(
        provider, source, inference_store, is_local, is_insecure
    )

    phrase_a_results = resource_client.search("quick", is_local)
    phrase_b_results = resource_client.search("transact", is_local)
    phrase_c_results = resource_client.search("dataset", is_local)
    phrase_d_results = resource_client.search("foo", is_local)

    counts = [
        len(phrase_a_results),
        len(phrase_b_results),
        len(phrase_c_results),
        len(phrase_d_results),
    ]

    assert counts == [4, 3, 1, 0]


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
def test_special_character_search(provider_source_fxt, is_local, is_insecure, request):
    custom_marks = [
        mark.name for mark in request.node.own_markers if mark.name != "parametrize"
    ]
    provider, source, inference_store = request.getfixturevalue(provider_source_fxt)(
        custom_marks
    )

    # Arranges the resources context following the Quickstart pattern
    resource_client = arrange_resources(
        provider, source, inference_store, is_local, is_insecure
    )

    phrase_a_results = resource_client.search("qui#ck!", is_local)
    phrase_b_results = resource_client.search("t*ran.saction!", is_local)
    phrase_c_results = resource_client.search("d,ata@se-t", is_local)

    assert (
        len(phrase_a_results) == 4
        and len(phrase_b_results) == 3
        and len(phrase_c_results) == 1
    )


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
def test_empty_query_to_search(provider_source_fxt, is_local, is_insecure, request):
    custom_marks = [
        mark.name for mark in request.node.own_markers if mark.name != "parametrize"
    ]
    provider, source, inference_store = request.getfixturevalue(provider_source_fxt)(
        custom_marks
    )

    # Arranges the resources context following the Quickstart pattern
    resource_client = arrange_resources(
        provider, source, inference_store, is_local, is_insecure
    )

    with pytest.raises(Exception, match="query must be string and cannot be empty"):
        phrase = resource_client.search(raw_query="", local=is_local)


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
def test_query_type_in_search(provider_source_fxt, is_local, is_insecure, request):
    custom_marks = [
        mark.name for mark in request.node.own_markers if mark.name != "parametrize"
    ]
    provider, source, inference_store = request.getfixturevalue(provider_source_fxt)(
        custom_marks
    )

    # Arranges the resources context following the Quickstart pattern
    resource_client = arrange_resources(
        provider, source, inference_store, is_local, is_insecure
    )

    with pytest.raises(
        TypeError, match="missing 1 required positional argument: 'raw_query'"
    ):
        phrase = resource_client.search(local=is_local)


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

    resource_client = ff.ResourceClient(local=is_local, insecure=is_insecure)
    resource_client.apply()

    return resource_client

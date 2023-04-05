import featureform as ff
import pytest

from featureform.resources import Entity, Feature, Label


@pytest.mark.parametrize(
    "provider_source_fxt,is_local,is_insecure",
    [
        pytest.param("local_provider_source", True, True, marks=pytest.mark.local),
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
            lambda r: isinstance(r, (Entity, Feature, Label)), ff.state().sorted_list()
        )
    )

    ff.clear_state()

    arrange_resources(
        provider, source, inference_store, is_local, is_insecure, is_class_api=True
    )
    class_syntax_state = list(
        filter(
            lambda r: isinstance(r, (Entity, Feature, Label)), ff.state().sorted_list()
        )
    )

    assert original_syntax_state == class_syntax_state


@pytest.mark.parametrize(
    "provider_source_fxt,is_local",
    [
        pytest.param("local_provider_source", True, marks=pytest.mark.local),
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


@pytest.mark.parametrize(
    "provider_source_fxt,is_local",
    [
        pytest.param("local_provider_source", True, marks=pytest.mark.local),
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
        pytest.param("local_provider_source", True, marks=pytest.mark.local),
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
        pytest.param("local_provider_source", True, marks=pytest.mark.local),
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

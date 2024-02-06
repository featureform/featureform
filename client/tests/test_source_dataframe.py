import featureform as ff
import pandas as pd
import pytest
from featureform.enums import FileFormat


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
def test_dataframe_for_name_variant_args(
    provider_source_fxt, is_local, is_insecure, request
):
    custom_marks = [
        mark.name for mark in request.node.own_markers if mark.name != "parametrize"
    ]
    provider, source, inference_store = request.getfixturevalue(provider_source_fxt)(
        custom_marks
    )

    transformation = arrange_transformation(provider, is_local)

    client = ff.Client(local=is_local, insecure=is_insecure)
    # If we're running in a hosted context, `apply` needs to be synchronous
    # to ensure resources are ready to test.
    client.apply(asynchronous=is_local)

    if is_local:
        source_df = client.dataframe(source.name, source.variant)
    else:
        source_df = client.dataframe(*source.name_variant())
    transformation_df = client.dataframe(*transformation.name_variant())

    assert isinstance(source_df, pd.DataFrame) and isinstance(
        transformation_df, (pd.DataFrame, pd.Series)
    )

    if is_local:
        client.impl.db.close()  # TODO automatically do this


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
def test_dataframe_for_source_args(provider_source_fxt, is_local, is_insecure, request):
    custom_marks = [
        mark.name for mark in request.node.own_markers if mark.name != "parametrize"
    ]
    provider, source, inference_store = request.getfixturevalue(provider_source_fxt)(
        custom_marks
    )

    transformation = arrange_transformation(provider, is_local)

    client = ff.Client(local=is_local, insecure=is_insecure)
    # If we're running in a hosted context, `apply` needs to be synchronous
    # to ensure resources are ready to test.
    client.apply(asynchronous=is_local)

    source_df = client.dataframe(source)
    transformation_df = client.dataframe(transformation)

    assert isinstance(source_df, pd.DataFrame) and isinstance(
        transformation_df, (pd.DataFrame, pd.Series)
    )

    if is_local:
        client.impl.db.close()  # TODO automatically do this


@pytest.fixture(autouse=True)
def before_and_after_each(setup_teardown):
    setup_teardown()
    yield
    setup_teardown()


def arrange_transformation(provider, is_local):
    if is_local:

        @provider.df_transformation(
            variant="quickstart", inputs=[("transactions", "quickstart")]
        )
        def average_user_transaction(transactions):
            return transactions.groupby("CustomerID")["TransactionAmount"].mean()

    else:

        @provider.sql_transformation(variant="quickstart")
        def average_user_transaction():
            return "SELECT customerid as user_id, avg(transactionamount) AS avg_transaction_amt FROM {{transactions.quickstart}} GROUP BY user_id"

    return average_user_transaction

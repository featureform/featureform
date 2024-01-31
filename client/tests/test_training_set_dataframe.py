import time
import pytest
import pandas as pd
import featureform as ff
from featureform import Client


@pytest.mark.parametrize(
    "provider_source_fxt,is_local,is_insecure",
    [
        pytest.param(
            "hosted_sql_provider_and_source",
            False,
            False,
            marks=pytest.mark.hosted,
        ),
        pytest.param(
            "hosted_sql_provider_and_source",
            False,
            True,
            marks=pytest.mark.docker,
        ),
    ],
)
def test_training_set_as_dataframe(provider_source_fxt, is_local, is_insecure, request):
    custom_marks = [
        mark.name for mark in request.node.own_markers if mark.name != "parametrize"
    ]
    provider, source, inference_store = request.getfixturevalue(provider_source_fxt)(
        custom_marks
    )

    # Arranges the resources context following the Quickstart pattern
    ts_name, ts_variant = arrange_resources(
        provider, source, inference_store, is_local, is_insecure
    )

    client = Client(local=is_local, insecure=is_insecure)
    ts_df = client.training_set(ts_name, ts_variant).dataframe()

    assert isinstance(ts_df, pd.DataFrame) and len(ts_df) > 0

    # TODO: Shouldn't have to do this
    if is_local:
        client.impl.db.close()


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

    return (training_set_name, training_set_variant)

import time
import featureform as ff
import pytest


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
def test_registering_resources_out_of_order(
    provider_source_fxt, is_local, is_insecure, request
):
    custom_marks = [
        mark.name for mark in request.node.own_markers if mark.name != "parametrize"
    ]
    provider, source, inference_store = request.getfixturevalue(provider_source_fxt)(
        custom_marks
    )
    training_set_name = "fraud_training"
    training_set_variant = "quickstart"

    # Arranges the resources context following the Quickstart pattern
    arrange_resources_out_of_order(
        provider,
        source,
        inference_store,
        is_local,
        training_set_name,
        training_set_variant,
    )

    resource_client = ff.ResourceClient(local=is_local, insecure=is_insecure)
    resource_client.apply(asynchronous=True)
    wait_for_training_set(
        resource_client, training_set_name, training_set_variant, is_local
    )

    training_set = resource_client.print_training_set(
        training_set_name, training_set_variant, is_local
    )

    if is_local:
        assert training_set is not None and training_set["status"] == "ready"
    else:
        assert (
            training_set is not None
            and training_set.status.Status._enum_type.values[
                training_set.status.status
            ].name
            == "READY"
        )


@pytest.fixture(autouse=True)
def before_and_after_each(setup_teardown):
    setup_teardown()
    yield
    setup_teardown()


def arrange_resources_out_of_order(
    provider, source, online_store, is_local, training_set_name, training_set_variant
):
    ff.register_training_set(
        training_set_name,
        training_set_variant,
        label=("fraudulent", "quickstart"),
        features=[("avg_transactions", "quickstart")],
    )

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

    feature_column = "TransactionAmount" if is_local else "avg_transaction_amt"
    label_column = "IsFraud" if is_local else "isfraud"
    inference_store = provider if is_local else online_store

    average_user_transaction.register_resources(
        entity="user",
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

    # register feature with no inference store
    average_user_transaction.register_resources(
        entity="user",
        entity_column="CustomerID" if is_local else "user_id",
        features=[
            {
                "name": "avg_transactions2",
                "variant": "quickstart",
                "column": feature_column,
                "type": "float32",
            },
        ],
    )

    source.register_resources(
        entity="user",
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

    ff.register_entity("user")


def wait_for_training_set(
    resource_client, training_set_name, training_set_variant, is_local
):
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

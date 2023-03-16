import featureform as ff
from featureform.resources import Model
import os
import pytest
import time

real_path = os.path.realpath(__file__)
dir_path = os.path.dirname(real_path)

@pytest.mark.parametrize(
    "is_local,is_insecure",
    [
        pytest.param(True, True, marks=pytest.mark.local),
        pytest.param(False, False, marks=pytest.mark.hosted),
        pytest.param(False, True, marks=pytest.mark.docker),
    ]
)
def test_adding_tags_and_properties_to_provider(is_local, is_insecure, request):
    name = "postgres_docs"
    tags = ["pg_offline_provider", "ff_team"]
    properties = {"primary_offline_provider": "true"}

    ff.register_postgres(
        name = name,
        description = "Example offline store store",
        team = "Featureform",
        host = "0.0.0.0",
        port = "5432",
        user = "postgres",
        password = "password",
        database = "postgres",
        tags=tags,
        properties=properties
    )

    resource_client = ff.ResourceClient(local=is_local, insecure=is_insecure)
    resource_client.apply()

    postgres = resource_client.get_provider(name, is_local)

    assert postgres["tags"] == tags and postgres["properties"] == properties


@pytest.mark.parametrize(
    "is_local,is_insecure",
    [
        pytest.param(True, True, marks=pytest.mark.local),
        pytest.param(False, False, marks=pytest.mark.hosted),
        pytest.param(False, True, marks=pytest.mark.docker),
    ]
)
def test_updating_tags_and_properties_for_provider(is_local, is_insecure, request):
    name = "postgres_docs"
    tags = ["pg_offline_provider", "ff_team"]
    properties = {"primary_offline_provider": "true"}

    ff.register_postgres(
        name = name,
        description = "Example offline store store",
        team = "Featureform",
        host = "0.0.0.0",
        port = "5432",
        user = "postgres",
        password = "password",
        database = "postgres",
        tags=tags,
        properties=properties
    )

    resource_client = ff.ResourceClient(local=is_local, insecure=is_insecure)
    resource_client.apply()

    ff.clear_state()

    additional_tags = ["training_data"]
    additional_properties = {"is_active": "true"}

    ff.register_postgres(
        name = name,
        description = "Example offline store store",
        team = "Featureform",
        host = "0.0.0.0",
        port = "5432",
        user = "postgres",
        password = "password",
        database = "postgres",
        tags=additional_tags,
        properties=additional_properties
    )
    resource_client.apply()
    postgres = resource_client.get_provider(name, is_local)

    assert set(postgres["tags"]) == set(tags + additional_tags) and postgres["properties"] == {**properties, **additional_properties}


@pytest.mark.parametrize(
    "is_local,is_insecure",
    [
        pytest.param(True, True, marks=pytest.mark.local),
        pytest.param(False, False, marks=pytest.mark.hosted),
        pytest.param(False, True, marks=pytest.mark.docker),
    ]
)
def test_adding_tags_and_properties_to_user(is_local, is_insecure, request):
    username = "ff_user"
    tags = ["primary_user"]
    properties = {"rotated_credentials": "yes"}

    ff.register_user(username,tags=tags, properties=properties)

    resource_client = ff.ResourceClient(local=is_local, insecure=is_insecure)
    resource_client.apply()

    user = resource_client.get_user(username, is_local)

    assert user["tags"] == tags and user["properties"] == properties


@pytest.mark.parametrize(
    "is_local,is_insecure",
    [
        pytest.param(True, True, marks=pytest.mark.local),
        pytest.param(False, False, marks=pytest.mark.hosted),
        pytest.param(False, True, marks=pytest.mark.docker),
    ]
)
def test_updating_tags_and_properties_for_user(is_local, is_insecure, request):
    username = "ff_user"
    tags = ["primary_user"]
    properties = {"rotated_credentials": "yes"}

    ff.register_user(username,tags=tags, properties=properties)

    resource_client = ff.ResourceClient(local=is_local, insecure=is_insecure)
    resource_client.apply()

    ff.clear_state()

    additional_tags = ["shared_credentials"]
    additional_properties = {"credential_location": "secret_vault"}

    ff.register_user(username,tags=additional_tags, properties=additional_properties)
    resource_client.apply()

    user = resource_client.get_user(username, is_local)

    assert set(user["tags"]) == set(tags + additional_tags) and user["properties"] == {**properties, **additional_properties}


@pytest.mark.parametrize(
    "provider_source_fxt,is_local,is_insecure",
    [
        pytest.param("local_provider_source", True, True, marks=pytest.mark.local),
        pytest.param("hosted_sql_provider_and_source", False, False, marks=pytest.mark.hosted),
        pytest.param("hosted_sql_provider_and_source", False, True, marks=pytest.mark.docker),
    ]
)
def test_adding_tags_and_properties_to_source(provider_source_fxt, is_local, is_insecure, request):
    custom_marks = [mark.name for mark in request.node.own_markers if mark.name != 'parametrize']
    provider, source, inference_store = request.getfixturevalue(provider_source_fxt)(custom_marks)

    name = "transactions"
    variant = "quickstart_v2"
    tags = ["fraud_project", "primary_source"]
    properties = {"project_type": "fraud_prediction"}

    if is_local:
        provider.register_file(
            name=name,
            variant=variant,
            description="A dataset of fraudulent transactions.",
            path=f"{dir_path}/test_files/input_files/transactions.csv",
            tags=tags,
            properties=properties
        )
    else:
        provider.register_table(
            name=name,
            table="Transactions", # This is the table's name in Postgres
            variant=variant,
            tags=tags,
            properties=properties
        )

    resource_client = ff.ResourceClient(local=is_local, insecure=is_insecure)
    resource_client.apply()

    source = resource_client.print_source(name, variant, is_local)

    assert source["tags"] == tags and source["properties"] == properties


@pytest.mark.parametrize(
    "provider_source_fxt,is_local,is_insecure",
    [
        pytest.param("local_provider_source", True, True, marks=pytest.mark.local),
        pytest.param("hosted_sql_provider_and_source", False, False, marks=pytest.mark.hosted),
        pytest.param("hosted_sql_provider_and_source", False, True, marks=pytest.mark.docker),
    ]
)
def test_updating_tags_and_properties_for_source(provider_source_fxt, is_local, is_insecure, request):
    custom_marks = [mark.name for mark in request.node.own_markers if mark.name != 'parametrize']
    provider, source, inference_store = request.getfixturevalue(provider_source_fxt)(custom_marks)

    name = "transactions"
    variant = "quickstart_v2"
    tags = ["fraud_project", "primary_source"]
    properties = {"project_type": "fraud_prediction"}

    if is_local:
        provider.register_file(
            name=name,
            variant=variant,
            description="A dataset of fraudulent transactions.",
            path=f"{dir_path}/test_files/input_files/transactions.csv",
            tags=tags,
            properties=properties
        )
    else:
        provider.register_table(
            name=name,
            table="Transactions", # This is the table's name in Postgres
            variant=variant,
            tags=tags,
            properties=properties
        )

    resource_client = ff.ResourceClient(local=is_local, insecure=is_insecure)
    resource_client.apply()

    ff.clear_state()

    additional_tags = ["v2"]
    additional_properties = {"version": "2"}

    if is_local:
        provider.register_file(
            name=name,
            variant=variant,
            description="A dataset of fraudulent transactions.",
            path=f"{dir_path}/test_files/input_files/transactions.csv",
            tags=additional_tags,
            properties=additional_properties
        )
    else:
        provider.register_table(
            name=name,
            table="Transactions", # This is the table's name in Postgres
            variant=variant,
            tags=additional_tags,
            properties=additional_properties
        )

    resource_client.apply()

    source = resource_client.print_source(name, variant, is_local)

    assert set(source["tags"]) == set(tags + additional_tags) and source["properties"] == {**properties, **additional_properties}


@pytest.mark.parametrize(
    "provider_source_fxt,is_local,is_insecure",
    [
        pytest.param("local_provider_source", True, True, marks=pytest.mark.local),
        pytest.param("hosted_sql_provider_and_source", False, False, marks=pytest.mark.hosted),
        pytest.param("hosted_sql_provider_and_source", False, True, marks=pytest.mark.docker),
    ]
)
def test_adding_tags_and_properties_to_transformation(provider_source_fxt, is_local, is_insecure, request):
    custom_marks = [mark.name for mark in request.node.own_markers if mark.name != 'parametrize']
    provider, source, inference_store = request.getfixturevalue(provider_source_fxt)(custom_marks)
    tags=["user_avg"]
    properties={"aggregate_type": "avg"}
    variant = "quickstart"
    if is_local:
        @provider.df_transformation(variant=variant, inputs=[("transactions", "quickstart")], tags=tags, properties=properties)
        def average_user_transaction(transactions):
            return transactions.groupby("CustomerID")["TransactionAmount"].mean()
    else:
        @provider.sql_transformation(variant=variant, tags=tags, properties=properties)
        def average_user_transaction():
            return "SELECT customerid as user_id, avg(transactionamount) as avg_transaction_amt from {{transactions.quickstart}} GROUP BY user_id"

    resource_client = ff.ResourceClient(local=is_local, insecure=is_insecure)
    resource_client.apply()

    source = resource_client.print_source("average_user_transaction", variant, is_local)

    assert source["tags"] == tags and source["properties"] == properties


@pytest.mark.parametrize(
    "provider_source_fxt,is_local,is_insecure",
    [
        pytest.param("local_provider_source", True, True, marks=pytest.mark.local),
        pytest.param("hosted_sql_provider_and_source", False, False, marks=pytest.mark.hosted),
        pytest.param("hosted_sql_provider_and_source", False, True, marks=pytest.mark.docker),
    ]
)
def test_updating_tags_and_properties_for_transformation(provider_source_fxt, is_local, is_insecure, request):
    custom_marks = [mark.name for mark in request.node.own_markers if mark.name != 'parametrize']
    provider, source, inference_store = request.getfixturevalue(provider_source_fxt)(custom_marks)
    tags=["user_avg"]
    properties={"aggregate_type": "avg"}
    variant = "quickstart"
    if is_local:
        @provider.df_transformation(variant=variant, inputs=[("transactions", "quickstart")], tags=tags, properties=properties)
        def average_user_transaction(transactions):
            return transactions.groupby("CustomerID")["TransactionAmount"].mean()
    else:
        @provider.sql_transformation(variant=variant, tags=tags, properties=properties)
        def average_user_transaction():
            return "SELECT customerid as user_id, avg(transactionamount) as avg_transaction_amt from {{transactions.quickstart}} GROUP BY user_id"

    resource_client = ff.ResourceClient(local=is_local, insecure=is_insecure)
    resource_client.apply()

    ff.clear_state()

    additional_tags = ["avg_user_transaction_v2"]
    additional_properties = {"version": "2"}

    if is_local:
        @provider.df_transformation(variant=variant, inputs=[("transactions", "quickstart")], tags=additional_tags, properties=additional_properties)
        def average_user_transaction(transactions):
            return transactions.groupby("CustomerID")["TransactionAmount"].mean()
    else:
        @provider.sql_transformation(variant=variant, tags=additional_tags, properties=additional_properties)
        def average_user_transaction():
            return "SELECT customerid as user_id, avg(transactionamount) as avg_transaction_amt from {{transactions.quickstart}} GROUP BY user_id"

    resource_client.apply()
    source = resource_client.print_source("average_user_transaction", variant, is_local)

    assert set(source["tags"]) == set(tags + additional_tags) and source["properties"] == {**properties, **additional_properties}


@pytest.mark.parametrize(
    "is_local,is_insecure",
    [
        pytest.param(True, True, marks=pytest.mark.local),
        pytest.param(False, False, marks=pytest.mark.hosted),
        pytest.param(False, True, marks=pytest.mark.docker),
    ]
)
def test_adding_tags_and_properties_to_entity(is_local, is_insecure, request):
    name = "cc_user"
    tags = ["customers", "user_ent"]
    properties = {"entity_name": "user", "is_user_data": "yes",}

    ff.register_entity(name, tags=tags, properties=properties)
    resource_client = ff.ResourceClient(local=is_local, insecure=is_insecure)
    resource_client.apply()

    entity = resource_client.get_entity(name, is_local)

    assert entity["tags"] == tags and entity["properties"] == properties


@pytest.mark.parametrize(
    "is_local,is_insecure",
    [
        pytest.param(True, True, marks=pytest.mark.local),
        pytest.param(False, False, marks=pytest.mark.hosted),
        pytest.param(False, True, marks=pytest.mark.docker),
    ]
)
def test_updating_tags_and_properties_for_entity(is_local, is_insecure, request):
    name = "cc_user"
    tags = ["customers", "user_ent"]
    properties = {"entity_name": "user", "is_user_data": "yes",}

    ff.register_entity(name, tags=tags, properties=properties)
    resource_client = ff.ResourceClient(local=is_local, insecure=is_insecure)
    resource_client.apply()

    ff.clear_state()

    additional_tags = ["entity_v2"]
    additional_properties = {"ent_version": "2", "updated_at": "2023-03-12"}

    ff.register_entity(name, tags=additional_tags, properties=additional_properties)
    resource_client.apply()

    entity = resource_client.get_entity(name, is_local)

    assert set(entity["tags"]) == set(tags + additional_tags) and entity["properties"] == {**properties, **additional_properties}


@pytest.mark.parametrize(
    "provider_source_fxt,is_local,is_insecure",
    [
        pytest.param("local_provider_source", True, True, marks=pytest.mark.local),
        pytest.param("hosted_sql_provider_and_source", False, False, marks=pytest.mark.hosted),
        pytest.param("hosted_sql_provider_and_source", False, True, marks=pytest.mark.docker),
    ]
)
def test_adding_tags_and_properties_to_feature(provider_source_fxt, is_local, is_insecure, request):
    custom_marks = [mark.name for mark in request.node.own_markers if mark.name != 'parametrize']
    provider, source, inference_store = request.getfixturevalue(provider_source_fxt)(custom_marks)
    tags = ["tag_1", "tag_2", "tag_3"]
    properties = {"prop_key_1": "prop_val_1", "prop_key_2": "prop_val_2"}
    # Arranges the resources context following the Quickstart pattern
    resource_client = arrange_resources(provider, source, inference_store, tags, properties, is_local, is_insecure)

    feature = resource_client.print_feature("avg_transactions", "quickstart", is_local)

    assert feature["tags"] == tags and feature["properties"] == properties


@pytest.mark.parametrize(
    "provider_source_fxt,is_local,is_insecure",
    [
        pytest.param("local_provider_source", True, True, marks=pytest.mark.local),
        pytest.param("hosted_sql_provider_and_source", False, False, marks=pytest.mark.hosted),
        pytest.param("hosted_sql_provider_and_source", False, True, marks=pytest.mark.docker),
    ]
)
def test_updating_tags_and_properties_for_feature(provider_source_fxt, is_local, is_insecure, request):
    custom_marks = [mark.name for mark in request.node.own_markers if mark.name != 'parametrize']
    provider, source, inference_store = request.getfixturevalue(provider_source_fxt)(custom_marks)
    tags = ["tag_1", "tag_2", "tag_3"]
    properties = {"prop_key_1": "prop_val_1", "prop_key_2": "prop_val_2"}
    # Arranges the resources context following the Quickstart pattern
    resource_client = arrange_resources(provider, source, inference_store, tags, properties, is_local, is_insecure)

    ff.clear_state()

    additional_tags = ["tag_4", "tag_5"]
    additional_properties = {"prop_key_3": "prop_val_3"}

    resource_client = arrange_resources(provider, source, inference_store, additional_tags, additional_properties, is_local, is_insecure)

    feature = resource_client.print_feature("avg_transactions", "quickstart", is_local)

    assert set(feature["tags"]) == set(tags + additional_tags) and feature["properties"] == {**properties, **additional_properties}


@pytest.mark.parametrize(
    "provider_source_fxt,is_local,is_insecure",
    [
        pytest.param("local_provider_source", True, True, marks=pytest.mark.local),
        pytest.param("hosted_sql_provider_and_source", False, False, marks=pytest.mark.hosted),
        pytest.param("hosted_sql_provider_and_source", False, True, marks=pytest.mark.docker),
    ]
)
def test_adding_tags_and_properties_to_label(provider_source_fxt, is_local, is_insecure, request):
    custom_marks = [mark.name for mark in request.node.own_markers if mark.name != 'parametrize']
    provider, source, inference_store = request.getfixturevalue(provider_source_fxt)(custom_marks)
    tags = ["tag_1", "tag_2", "tag_3"]
    properties = {"prop_key_1": "prop_val_1", "prop_key_1": "prop_val_2"}
    # Arranges the resources context following the Quickstart pattern
    resource_client = arrange_resources(provider, source, inference_store, tags, properties, is_local, is_insecure)

    label = resource_client.print_label("fraudulent", "quickstart", is_local)

    assert label["tags"] == tags and label["properties"] == properties


@pytest.mark.parametrize(
    "provider_source_fxt,is_local,is_insecure",
    [
        pytest.param("local_provider_source", True, True, marks=pytest.mark.local),
        pytest.param("hosted_sql_provider_and_source", False, False, marks=pytest.mark.hosted),
        pytest.param("hosted_sql_provider_and_source", False, True, marks=pytest.mark.docker),
    ]
)
def test_updating_tags_and_properties_for_label(provider_source_fxt, is_local, is_insecure, request):
    custom_marks = [mark.name for mark in request.node.own_markers if mark.name != 'parametrize']
    provider, source, inference_store = request.getfixturevalue(provider_source_fxt)(custom_marks)
    tags = ["tag_1", "tag_2", "tag_3"]
    properties = {"prop_key_1": "prop_val_1", "prop_key_1": "prop_val_2"}
    # Arranges the resources context following the Quickstart pattern
    resource_client = arrange_resources(provider, source, inference_store, tags, properties, is_local, is_insecure)

    ff.clear_state()

    additional_tags = ["tag_4", "tag_5"]
    additional_properties = {"prop_key_3": "prop_val_3"}

    resource_client = arrange_resources(provider, source, inference_store, additional_tags, additional_properties, is_local, is_insecure)

    label = resource_client.print_label("fraudulent", "quickstart", is_local)

    assert set(label["tags"]) == set(tags + additional_tags) and label["properties"] == {**properties, **additional_properties}


@pytest.mark.parametrize(
    "provider_source_fxt,is_local,is_insecure",
    [
        pytest.param("local_provider_source", True, True, marks=pytest.mark.local),
        pytest.param("hosted_sql_provider_and_source", False, False, marks=pytest.mark.hosted),
        pytest.param("hosted_sql_provider_and_source", False, True, marks=pytest.mark.docker),
    ]
)
def test_adding_tags_and_properties_to_training_set(provider_source_fxt, is_local, is_insecure, request):
    custom_marks = [mark.name for mark in request.node.own_markers if mark.name != 'parametrize']
    provider, source, inference_store = request.getfixturevalue(provider_source_fxt)(custom_marks)
    tags = ["tag_1", "tag_2", "tag_3"]
    properties = {"prop_key_1": "prop_val_1", "prop_key_1": "prop_val_2"}
    # Arranges the resources context following the Quickstart pattern
    resource_client = arrange_resources(provider, source, inference_store, tags, properties, is_local, is_insecure, True)

    training_set = resource_client.print_training_set("fraud_training", "quickstart", is_local)

    assert training_set["tags"] == tags and training_set["properties"] == properties


@pytest.mark.parametrize(
    "provider_source_fxt,is_local,is_insecure",
    [
        pytest.param("local_provider_source", True, True, marks=pytest.mark.local),
        pytest.param("hosted_sql_provider_and_source", False, False, marks=pytest.mark.hosted),
        pytest.param("hosted_sql_provider_and_source", False, True, marks=pytest.mark.docker),
    ]
)
def test_updating_tags_and_properties_for_training_set(provider_source_fxt, is_local, is_insecure, request):
    custom_marks = [mark.name for mark in request.node.own_markers if mark.name != 'parametrize']
    provider, source, inference_store = request.getfixturevalue(provider_source_fxt)(custom_marks)
    tags = ["tag_1", "tag_2", "tag_3"]
    properties = {"prop_key_1": "prop_val_1", "prop_key_1": "prop_val_2"}
    # Arranges the resources context following the Quickstart pattern
    resource_client = arrange_resources(provider, source, inference_store, tags, properties, is_local, is_insecure, True)

    ff.clear_state()

    additional_tags = ["tag_4", "tag_5"]
    additional_properties = {"prop_key_3": "prop_val_3"}

    resource_client = arrange_resources(provider, source, inference_store, additional_tags, additional_properties, is_local, is_insecure, True)

    training_set = resource_client.print_training_set("fraud_training", "quickstart", is_local)

    assert set(training_set["tags"]) == set(tags + additional_tags) and training_set["properties"] == {**properties, **additional_properties}


@pytest.fixture(autouse=True)
def before_and_after_each(setup_teardown):
    setup_teardown()
    yield
    setup_teardown()


def arrange_resources(provider, source, online_store, tags, properties, is_local, is_insecure, should_register_training_set=False):
    if is_local:
        @provider.df_transformation(variant="quickstart", inputs=[("transactions", "quickstart")])
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
            {"name": "avg_transactions", "variant": "quickstart", "column": feature_column, "type": "float32", "tags": tags, "properties": properties},
        ],
    )

    source.register_resources(
        entity=user,
        entity_column="CustomerID" if is_local else "customerid",
        labels=[
            {"name": "fraudulent", "variant": "quickstart", "column": label_column, "type": "bool", "tags": tags, "properties": properties},
        ],
    )

    training_set_name = "fraud_training"
    training_set_variant = "quickstart"

    if should_register_training_set:
        ff.register_training_set(
            training_set_name, training_set_variant,
            label=("fraudulent", "quickstart"),
            features=[("avg_transactions", "quickstart")],
            tags=tags,
            properties=properties
        )

    resource_client = ff.ResourceClient(local=is_local, insecure=is_insecure)
    resource_client.apply()

    if not is_local and should_register_training_set:
        start = time.time()
        while True:
            time.sleep(3)
            ts = resource_client.get_training_set(training_set_name, training_set_variant)
            elapsed_wait = time.time() - start
            if (elapsed_wait >= 60) and ts.status != "READY":
                print(f"Wait time for training set status exceeded; status is {ts.status}")
                break
            elif ts.status == "READY":
                print(f"Training set is ready")
                break
            else:
                print(f"Training set status is currently {ts.status} after {elapsed_wait} seconds ...")
                continue

    return resource_client

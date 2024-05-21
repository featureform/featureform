import os
import random
import string

import featureform as ff
from behave import when, then
from dotenv import load_dotenv
from featureform.enums import ScalarType


@when('I register an offline provider of type "{offline_provider_type}"')
def step_impl(context, offline_provider_type):
    version = random_word(5)

    load_dotenv("../../../.env")

    if offline_provider_type.lower() == "postgres":
        context.offline_provider = ff.register_postgres(
            name=f"postgres_{version}",
            host=os.getenv("POSTGRES_HOST", "host.docker.internal"),
            port=os.getenv("POSTGRES_PORT", "5432"),
            user=os.getenv("POSTGRES_USER", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD", "password"),
            database=os.getenv("POSTGRES_DB", "postgres"),
        )
    elif offline_provider_type.lower() == "snowflake":
        raise NotImplementedError("Snowflake registration is not implemented")
    elif offline_provider_type.lower() == "bigquery":
        raise NotImplementedError("BigQuery registration is not implemented")
    elif offline_provider_type.lower() == "spark":
        """
        Need to register spark with s3 and databricks
        TODO: modify this so you can create spark with different Filestore & Executor
        """

        # create a s3 filestore
        context.execute_steps(
            """
            when I register "s3" filestore with bucket "featureform-spark-testing" and root path "behave"
            """
        )

        # register databricks and spark
        context.execute_steps(
            """
            when I register databricks
            """
        )
        context.offline_provider = context.spark

    else:
        raise ValueError(f"Unknown offline provider {offline_provider_type}")

    context.client.apply(asynchronous=False, verbose=True)


@when('I register an online provider of type "{online_provider_type}"')
def step_impl(context, online_provider_type):
    version = random_word(5)
    load_dotenv("../../.env")

    if online_provider_type.lower() == "redis":
        context.online_provider = ff.register_redis(
            name=f"redis_{version}",
            host=os.getenv("REDIS_HOST", "host.docker.internal"),
            port=int(os.getenv("REDIS_PORT", "6379")),
        )
    else:
        raise ValueError(f"Unknown offline provider {online_provider_type}")

    context.client.apply(asynchronous=False, verbose=True)


@when('I register a dataset located at "{dataset_path}"')
def step_impl(context, dataset_path):
    if (
        context.offline_provider._OfflineProvider__provider.config.type()
        == "SPARK_OFFLINE"
    ):
        dataset = context.offline_provider.register_file(
            name="transactions",
            file_path=dataset_path,
        )
    else:
        dataset = context.offline_provider.register_table(
            name="transactions",
            table=dataset_path,
        )

    context.client.apply(asynchronous=False, verbose=True)
    context.dataset = dataset


@when(
    'I register a feature on "{feature_column}" with type "{feature_type}" with "{entity_column}", "{timestamp_column}", and "{label_column}"'
)
def step_impl(
    context, feature_column, feature_type, entity_column, timestamp_column, label_column
):
    if timestamp_column == "empty":
        feature_dataset = context.dataset[[entity_column, feature_column]]
        label_dataset = context.dataset[[entity_column, label_column]]
    else:
        feature_dataset = context.dataset[
            [entity_column, feature_column, timestamp_column]
        ]
        label_dataset = context.dataset[[entity_column, label_column, timestamp_column]]

    feature_type = "" if feature_type == "null" else feature_type.lower()

    @ff.entity
    class User:
        user_feature = ff.Feature(
            feature_dataset,
            type=ScalarType(
                feature_type
            ),  # converts the string into Featureform type
            inference_store=context.online_provider,
        )
        user_label = ff.Label(
            label_dataset,
            type=ff.Bool,
        )

    context.client.apply(asynchronous=False, verbose=True)
    context.user_feature = User.user_feature
    context.user_label = User.user_label


@then("I can register a training set based on the feature")
def step_impl(context):
    training_set = ff.register_training_set(
        name="fraud_training_set",
        features=[context.user_feature],
        label=context.user_label,
    )
    context.client.apply(asynchronous=False, verbose=True)
    context.training_set = training_set


@then("I cannot serve the non-existing feature with the model")
def step_impl(context):
    """
    This step expects an error because the feature doesn't exist.
    If it doesn't error while serving the non-existing feature, it will raise an error.
    """

    context.model = "fraud-model"
    try:
        _ = context.client.features(
            [(context.user_feature.name, "non-existent")],
            {"user": "C5841053"},
            model=context.model,
        )
    except Exception as e:
        if "metadata lookup failed" in str(e):
            return
        else:
            raise ValueError(f"Unexpected error: {e}")

    raise ValueError("Expected an error but none was raised")


@then(
    'I can serve the registered feature with the model for "{user}" with "{expected_value}"'
)
def step_impl(context, user, expected_value):
    context.model = "fraud-model"
    feature = context.client.features(
        [(context.user_feature.name, context.user_feature.variant)],
        {"user": user},
        model=context.model,
    )

    assert len(feature) == 1, f"Expected 1 feature but got {len(feature)}"
    assert feature[0] == float(
        expected_value
    ), f"Expected {expected_value} but got {feature[0]}"


@then("I cannot serve the non-existing training set with the model")
def step_impl(context):
    """
    This step expects an error because the training set doesn't exist.
    If it doesn't error while serving the non-existing training set, it will raise an error.
    """

    context.model = "fraud-model"
    try:
        dataset = context.client.training_set(
            name=context.training_set.name,
            variant="non-existent",
            model=context.model,
        )

        training_dataset = dataset.repeat(10).shuffle(1000).batch(8)
        for i, _ in enumerate(training_dataset):
            if i >= 1:
                return

    except Exception as e:
        if "Key Not Found" in str(e):
            return
        else:
            raise ValueError(f"Unexpected error: {e}")

    raise ValueError("Expected an error but none was raised")


@then("I can serve the registered training set with the model")
def step_impl(context):
    context.model = "fraud-model"
    dataset = context.client.training_set(
        context.training_set,
        model=context.model,
    )

    # TODO: check the output of the dataset
    training_dataset = dataset.repeat(10).shuffle(1000).batch(8)
    for i, feature_batch in enumerate(training_dataset):
        if i >= 1:
            return
        print(feature_batch.to_list())


def random_word(length):
    letters = string.ascii_lowercase
    return "".join(random.choice(letters) for i in range(length))

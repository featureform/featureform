import os

import featureform as ff
from featureform.enums import ScalarType
from behave import when, then


@when("I register a '{offline_provider}'")
def step_impl(context, offline_provider):
    from dotenv import load_dotenv

    load_dotenv("../../.env")

    if offline_provider.lower() == "postgres":
        context.offline_provider = ff.register_postgres(
            name="postgres",
            host=os.getenv("POSTGRES_HOST", "host.docker.internal"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            user=os.getenv("POSTGRES_USER", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD", "password"),
            database=os.getenv("POSTGRES_DB", "postgres"),
        )
    elif offline_provider.lower() == "snowflake":
        raise NotImplementedError("Snowflake registration is not implemented")
    elif offline_provider.lower() == "bigquery":
        raise NotImplementedError("BigQuery registration is not implemented")
    elif offline_provider.lower() == "spark":
        raise NotImplementedError("Spark registration is not implemented")
    else:
        raise ValueError(f"Unknown offline provider {offline_provider}")

    context.client.apply(asynchronous=False, verbose=True)


@when("I register a dataset located at '{dataset_path}'")
def step_impl(context, dataset_path):
    if context.offline_provider.provider.config.type() == "SPARK_OFFLINE":
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


@when("I register a feature base on the colum '{column_name}' with type '{feature_type}'")
def step_impl(context, column_name, feature_type):
    @ff.entity
    class User:
        amount = ff.Feature(
            context.dataset[column_name],
            type=ScalarType(feature_type.lower()),  # converts the string into Featureform type
            inference_store=context.online_provider,
        )
        fraudulent = ff.Label(
            context.dataset["IsFraud"],
            type=ff.Bool,
        )

    context.client.apply(asynchronous=False, verbose=True)
    context.feature = User.amount
    context.label = User.fraudulent


@then("I can register a training set based on the feature")
def step_impl(context):
    training_set = context.client.register_training_set(
        name="fraud_training",
        features=[context.feature],
        label=context.label,
    )
    context.client.apply(asynchronous=False, verbose=True)
    context.training_set = training_set


@then("I can serve the non-existing training set with the model")
def step_impl(context):
    context.model = "fraud-model"
    dataset = context.client.training_set(
        name="fraud_training",
        variant="non-existent",
        model=context.model,
    )

    training_dataset = dataset.repeat(10).shuffle(1000).batch(8)
    for i, feature_batch in enumerate(training_dataset):
        if i >= 1:
            return
        print(feature_batch.to_list())
    

@then("I can serve the registered training set with the model")
def step_impl(context):
    context.model = "fraud-model"
    dataset = context.client.training_set(
        context.training_set,
        model=context.model,
    )

    training_dataset = dataset.repeat(10).shuffle(1000).batch(8)
    for i, feature_batch in enumerate(training_dataset):
        if i >= 1:
            return
        print(feature_batch.to_list())

import os
import subprocess
from time import sleep

from behave import given, when, then

import featureform as ff
from featureform import ResourceClient


@given("Featureform is installed")
def step_impl(context):
    output = subprocess.check_output(["pip", "list"], text=True)
    assert "featureform" in output


@given("Featureform is hosted on k8s")
def step_impl(context):
    host = os.getenv("FEATUREFORM_HOST")
    context.rc = ResourceClient(host=host)
    assert context.rc != None


@when("we register a Spark provider")
def step_impl(context):
    args = {
                "name": f"spark-{context.name_suffix}",
                "description": "test",
                "team": "featureform",
                "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
                "aws_secret_access_key": os.getenv("AWS_SECRET_KEY"),
                "bucket_path": os.getenv("S3_BUCKET_PATH"),
                "bucket_region": os.getenv("S3_BUCKET_REGION"),
                "emr_cluster_id": os.getenv("AWS_EMR_CLUSTER_ID"),
                "emr_cluster_region": os.getenv("AWS_EMR_CLUSTER_REGION"),
            }
    context.spark = ff.register_spark(**args)
    context.rc.apply()

    spark_provider = context.rc.get_provider(args["name"])
    assert spark_provider


@when("a Spark parquet file is registered")
def step_impl(context):
    file_name = f"transaction_short_1"
    context.file = context.spark.register_parquet_file(
        name=file_name,
        variant="e2e_testing",
        owner="featureformer",
        file_path="s3://featureform-spark-testing/featureform/source_datasets/transaction_short/",
    )
    context.rc.apply()

    file = context.rc.get_source(file_name)
    assert file.name == file_name


@when("Spark SQL transformation is registered")
def step_impl(context):
    @context.spark.sql_transformation(name=f"sql_transaction_transformation_{context.name_suffix}", variant="e2e_testing")
    def average_user_score():
        """the average score for a user"""
        return "SELECT CustomerID as user_id, avg(TransactionAmount) as avg_transaction_amt from {{{{ transaction_short_1.e2e_testing }}}} GROUP BY user_id"

    context.user = ff.register_entity("user")

    feature_name = f"avg_transaction_{context.name_suffix}"
    average_user_score.register_resources(
        entity=context.user,
        owner=f"featureformer",
        entity_column="user_id",
        inference_store=context.redis,
        features=[
            {"name": feature_name, "variant": "e2e-testing", "column": "avg_transaction_amt", "type": "float32"},
        ],
    )

    context.rc.apply()

    feature = context.rc.get_feature(feature_name)
    assert feature.name == feature_name


@when("a label is registered")
def step_impl(context):
    context.file.register_resources(
        entity=context.user,
        owner=f"featureformer_{context.name_suffix}",
        entity_column="CustomerID",
        labels=[
            {"name": f"fraudulent_{context.name_suffix}", "variant": "e2e-testing", "column": "isfraud", "type": "bool"},
        ],
    )

    context.rc.apply()


@when("training set is registered")
def step_impl(context):
    ff.register_training_set(
        f"fraud_training_{context.name_suffix}", "e2e-testing",
        owner=f"featureformer_{context.name_suffix}",
        label=(f"fraudulent_{context.name_suffix}", "e2e-testing"),
        features=[(f"avg_transaction_{context.name_suffix}", "e2e-testing")],
    )

    context.rc.apply()


@then("we can serve the training set")
def step_impl(context):
    waiting_period = 120
    print(f"Waiting for {waiting_period} seconds")
    sleep(waiting_period)

    client = ff.ServingClient()
    dataset = client.training_set(f"fraud_training_{context.name_suffix}", "e2e-testing")
    training_dataset = dataset.repeat(10).shuffle(1000).batch(8)
    
    dataset_size = 0
    for i, feature_batch in enumerate(training_dataset):
        dataset_size += len(feature_batch.to_list())
    
    assert dataset_size == 8000

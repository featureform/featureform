#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

import datetime
import os
import platform
import shutil
import sys

import dill
import pandas as pd
import pytest

sys.path.insert(0, "client/src/")

collect_ignore = ["embeddinghub"]

import featureform as ff
from featureform.register import (
    Registrar,
    OfflineSparkProvider,
    ColumnSourceRegistrar,
)
from featureform.resources import (
    AWSStaticCredentials,
    FileStore,
    GCPCredentials,
    AzureFileStoreConfig,
    DatabricksCredentials,
    EMRCredentials,
    S3StoreConfig,
    SparkConfig,
    SparkCredentials,
    Provider,
    PrimaryData,
    SQLTransformation,
    DFTransformation,
    SQLTable,
)

from featureform.type_objects import (
    SourceVariantResource,
)
from featureform.enums import FileFormat
from featureform.deploy import (
    DOCKER_CONFIG,
    DockerDeployment,
)

real_path = os.path.realpath(__file__)
dir_path = os.path.dirname(real_path)

pytest_plugins = []


def pytest_configure(config):
    import requests

    url = "https://featureform-demo-files.s3.amazonaws.com/transactions_short.csv"
    response = requests.get(url)
    with open("transactions.csv", "wb") as file:
        file.write(response.content)

    url = "https://featureform-demo-files.s3.amazonaws.com/Iris.csv"
    response = requests.get(url)
    with open("iris.csv", "wb") as file:
        file.write(response.content)


@pytest.fixture(scope="module")
def spark_provider(ff_registrar):
    databricks = DatabricksCredentials(
        username="a", password="b", cluster_id="abcd-123def-ghijklmn"
    )
    azure_blob = AzureFileStoreConfig(
        account_name="", account_key="", container_name="", root_path=""
    )

    config = SparkConfig(
        executor_type=databricks.type(),
        executor_config=databricks.config(),
        store_type=azure_blob.store_type(),
        store_config=azure_blob.config(),
    )
    provider = Provider(
        name="spark",
        function="OFFLINE",
        description="",
        team="",
        config=config,
        tags=[],
        properties={},
    )

    return OfflineSparkProvider(ff_registrar, provider)


@pytest.fixture(scope="module")
def avg_user_transaction():
    def average_user_transaction(df):
        """doc string"""
        from pyspark.sql.functions import avg

        df.groupBy("CustomerID").agg(
            avg("TransactionAmount").alias("average_user_transaction")
        )
        return df

    return average_user_transaction


@pytest.fixture(scope="module")
def ff_registrar():
    r = Registrar()
    r.set_default_owner("tester")
    return r


@pytest.fixture(scope="module")
def primary_source_sql_table(ff_registrar):
    src = SourceVariantResource(
        name="primary",
        variant="default",
        definition=PrimaryData(location=SQLTable(name="tableName")),
        owner="tester",
        provider="spark",
        description="doc string",
        tags=[],
        properties={},
    )
    colum_src = ColumnSourceRegistrar(ff_registrar, src)
    return [colum_src]


@pytest.fixture(scope="module")
def primary_source_file_store(ff_registrar):
    src = SourceVariantResource(
        name="primary",
        variant="default",
        definition=PrimaryData(location=FileStore(path_uri="path")),
        owner="tester",
        provider="spark",
        description="doc string",
        tags=[],
        properties={},
    )
    colum_src = ColumnSourceRegistrar(ff_registrar, src)
    return [colum_src]


@pytest.fixture(scope="module")
def sql_transformation_src(ff_registrar):
    src = SourceVariantResource(
        name="sql_transformation",
        variant="default",
        definition=SQLTransformation("SELECT * FROM {{ name.variant }}"),
        owner="tester",
        provider="spark",
        description="doc string",
        tags=[],
        properties={},
    )
    colum_src = ColumnSourceRegistrar(ff_registrar, src)
    return [colum_src]


@pytest.fixture(scope="module")
def tuple_inputs():
    return [("name", "variant")]


@pytest.fixture(scope="module")
def df_transformation_src(
    ff_registrar,
):
    def test_func():
        return True

    query = dill.dumps(test_func.__code__)
    source_text = dill.source.getsource(test_func)
    src = SourceVariantResource(
        name="sql_transformation",
        variant="default",
        definition=DFTransformation(
            query=query, inputs=[("name", "variant")], source_text=source_text
        ),
        owner="tester",
        provider="spark",
        description="doc string",
        tags=[],
        properties={},
    )
    colum_src = ColumnSourceRegistrar(ff_registrar, src)
    return [colum_src]


@pytest.fixture(scope="module")
def aws_credentials():
    return AWSStaticCredentials("id", "secret")


@pytest.fixture(scope="module")
def s3_config(aws_credentials):
    config = S3StoreConfig(
        "bucket_path", "bucket_region", aws_credentials, path="path_to_file/"
    )

    expected_config = {
        "Credentials": aws_credentials.config(),
        "BucketRegion": "bucket_region",
        "BucketPath": "bucket_path",
        "Path": "path_to_file/",
    }

    return config, expected_config


@pytest.fixture()
def s3_config_slash_ending(aws_credentials):
    config = S3StoreConfig("bucket_path/", "bucket_region", aws_credentials)

    return config, {}


@pytest.fixture()
def s3_config_slash(aws_credentials):
    config = S3StoreConfig("/", "bucket_region", aws_credentials)

    return config, {}


@pytest.fixture(scope="module")
def azure_file_config():
    config = AzureFileStoreConfig(
        "account_name", "account_key", "container_name", "root_path"
    )

    expected_config = {
        "AccountName": "account_name",
        "AccountKey": "account_key",
        "ContainerName": "container_name",
        "Path": "root_path",
    }

    return config, expected_config


@pytest.fixture(scope="module")
def databricks_config():
    config = DatabricksCredentials(
        username="username",
        password="password",
        cluster_id="abcd-123def-ghijklmn",
    )

    expected_config = {
        "Username": "username",
        "Password": "password",
        "Host": "",
        "Token": "",
        "Cluster": "abcd-123def-ghijklmn",
    }
    return config, expected_config


@pytest.fixture(scope="module")
def emr_config(aws_credentials):
    config = EMRCredentials("emr_cluster_id", "emr_cluster_region", aws_credentials)

    expected_config = {
        "Credentials": aws_credentials.config(),
        "ClusterName": "emr_cluster_id",
        "ClusterRegion": "emr_cluster_region",
    }

    return config, expected_config


@pytest.fixture(scope="module")
def spark_executor():
    master = "local"
    deploy_mode = "cluster"
    python_version = "3.9.16"

    config = SparkCredentials(master, deploy_mode, python_version)

    expected_config = {
        "Master": master,
        "DeployMode": deploy_mode,
        "PythonVersion": python_version,
        "YarnSite": "",
        "CoreSite": "",
    }

    return config, expected_config


@pytest.fixture(scope="module")
def spark_executor_incorrect_deploy_mode():
    master = "local"
    deploy_mode = "featureform"
    python_version = "3.9.16"

    config = SparkCredentials(master, deploy_mode, python_version)

    expected_config = {
        "Master": master,
        "DeployMode": deploy_mode,
        "PythonVersion": python_version,
    }

    return config, expected_config


@pytest.fixture(scope="module")
def databricks():
    return DatabricksCredentials(
        username="a", password="b", cluster_id="abcd-123def-ghijklmn"
    )


@pytest.fixture(scope="module")
def emr(aws_credentials):
    return EMRCredentials("emr_cluster_id", "emr_cluster_region", aws_credentials)


@pytest.fixture(scope="module")
def azure_blob():
    return AzureFileStoreConfig(
        account_name="", account_key="", container_name="", root_path=""
    )


@pytest.fixture(scope="module")
def s3(aws_credentials):
    return S3StoreConfig("bucket_path", "bucket_region", aws_credentials)


@pytest.fixture(scope="module")
def serving_client():
    def get_clients_for_context(is_local, is_insecure):
        return ff.ServingClient(local=is_local, insecure=is_insecure)

    return get_clients_for_context


@pytest.fixture(scope="module")
def setup_teardown():
    def clear_state_and_reset():
        ff.clear_state()
        shutil.rmtree(".featureform", onerror=del_rw)

    return clear_state_and_reset


def del_rw(action, name, exc):
    try:
        os.remove(name)
    except OSError as e:
        print(f"Error: {e}")


@pytest.fixture(scope="module")
def hosted_sql_provider_and_source():
    def get_hosted(custom_marks, file_format=FileFormat.CSV.value):
        ff.register_user("test_user").make_default_owner()

        provider = ff.register_postgres(
            name="postgres-quickstart",
            # The host name for postgres is different between Docker and Minikube
            host=(
                "host.docker.internal"
                if "docker" in custom_marks
                else "quickstart-postgres"
            ),
            port="5432",
            user="postgres",
            password="password",
            database="postgres",
            description="A Postgres deployment we created for the Featureform quickstart",
        )

        redis = ff.register_redis(
            name="redis-quickstart",
            # The host name for postgres is different between Docker and Minikube
            host=(
                "host.docker.internal"
                if "docker" in custom_marks
                else "quickstart-redis"
            ),
            port=6379,
            password="password",
        )

        source = provider.register_table(
            name="transactions",
            table="Transactions",  # This is the table's name in Postgres
            variant="quickstart",
        )

        return (provider, source, redis)

    return get_hosted


@pytest.fixture(scope="module")
def docker_deployment_config():
    environment_variables = {}
    featureform_config = DOCKER_CONFIG(
        name="featureform",
        image="featureformcom/featureform:latest",
        env=environment_variables,
        port={"7878/tcp": 7878, "80/tcp": 80},
        detach_mode=True,
    )
    return [featureform_config]


@pytest.fixture(scope="module")
def docker_quickstart_deployment_config():
    environment_variables = {}
    featureform_config = DOCKER_CONFIG(
        name="featureform",
        image="featureformcom/featureform:latest",
        env=environment_variables,
        port={"7878/tcp": 7878, "80/tcp": 80},
        detach_mode=True,
    )
    quickstart_postgres = DOCKER_CONFIG(
        name="quickstart-postgres",
        image="featureformcom/postgres",
        env={},
        port={"5432/tcp": 5432},
        detach_mode=True,
    )
    quickstart_redis = DOCKER_CONFIG(
        name="quickstart-redis",
        image="redis:latest",
        env={},
        port={"6379/tcp": 6379},
        detach_mode=True,
    )
    return [featureform_config, quickstart_postgres, quickstart_redis]


@pytest.fixture(scope="module")
def docker_deployment():
    return DockerDeployment(False)


@pytest.fixture(scope="module")
def docker_quickstart_deployment():
    return DockerDeployment(True)


@pytest.fixture(scope="module")
def docker_quickstart_deployment_with_clickhouse():
    return DockerDeployment(True, clickhouse=True)


@pytest.fixture(scope="module")
def docker_deployment_status():
    return None


@pytest.fixture(scope="module")
def spark_session():
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("test").master("local").getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture(scope="module")
def gcp_credentials():
    return GCPCredentials(
        project_id="project_id",
        credentials_path=f"{dir_path}/client/tests/test_files/bigquery_dummy_credentials.json",
    )


@pytest.fixture(scope="module")
def multi_feature(primary_source_sql_table, features_dataframe):
    return ff.MultiFeature(
        dataset=primary_source_sql_table[0],
        df=features_dataframe,
        entity_column="entity",
        timestamp_column="ts",
    )


@pytest.fixture(scope="module")
def client():
    return ff.Client(insecure=True, local=True)


@pytest.fixture(scope="module")
def data_dict():
    return {
        "entity": [7, 8],
        "a": [1, 4],
        "b": [2, 5],
        "c": [3, 6],
        "ts": [datetime.datetime.now(), datetime.datetime.now()],
    }


@pytest.fixture(scope="module")
def features_dataframe(data_dict):
    return pd.DataFrame(data_dict)

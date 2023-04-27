import os
import sys
import stat
import shutil
from tempfile import NamedTemporaryFile

import dill
import pytest

sys.path.insert(0, "client/src/")


from featureform.register import (
    Registrar,
    OfflineSparkProvider,
    ColumnSourceRegistrar,
)
from featureform.resources import (
    AWSCredentials,
    AzureFileStoreConfig,
    DatabricksCredentials,
    EMRCredentials,
    S3StoreConfig,
    SparkConfig,
    SparkCredentials,
    Provider,
    PrimaryData,
    Location,
    Source,
    SQLTransformation,
    DFTransformation,
)
from featureform.enums import FileFormat
import featureform as ff

real_path = os.path.realpath(__file__)
dir_path = os.path.dirname(real_path)

pytest_plugins = [
    "connection_test",
]


@pytest.fixture(scope="module")
def spark_provider(ff_registrar):
    databricks = DatabricksCredentials(username="a", password="b", cluster_id="c_id")
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
def primary_dataset(ff_registrar):
    src = Source(
        name="primary",
        variant="default",
        definition=PrimaryData(location=Location("tableName")),
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
    src = Source(
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
    src = Source(
        name="sql_transformation",
        variant="default",
        definition=DFTransformation(query, inputs=[("name", "variant")]),
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
    return AWSCredentials("id", "secret")


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
        username="username", password="password", cluster_id="cluster_id"
    )

    expected_config = {
        "Username": "username",
        "Password": "password",
        "Host": "",
        "Token": "",
        "Cluster": "cluster_id",
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
    python_version = "3.7.16"

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
    python_version = "3.7.16"

    config = SparkCredentials(master, deploy_mode, python_version)

    expected_config = {
        "Master": master,
        "DeployMode": deploy_mode,
        "PythonVersion": python_version,
    }

    return config, expected_config


@pytest.fixture(scope="module")
def databricks():
    return DatabricksCredentials(username="a", password="b", cluster_id="c_id")


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
def local_provider_source():
    # empty param to match the signature of the other fixtures
    def get_local(_, file_format=FileFormat.CSV.value):
        ff.register_user("test_user").make_default_owner()
        provider = ff.register_local()
        source = provider.register_file(
            name="transactions",
            variant="quickstart",
            description="A dataset of fraudulent transactions.",
            path=f"{dir_path}/test_files/input_files/transactions.{file_format}",
        )
        return (provider, source, None)

    return get_local


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
    if os.path.exists(name):
        os.chmod(name, stat.S_IWRITE)
        os.remove(name)


@pytest.fixture(scope="module")
def hosted_sql_provider_and_source():
    def get_hosted(custom_marks):
        ff.register_user("test_user").make_default_owner()

        postgres_host = (
            "host.docker.internal"
            if "docker" in custom_marks
            else "quickstart-postgres"
        )
        redis_host = (
            "host.docker.internal" if "docker" in custom_marks else "quickstart-redis"
        )

        provider = ff.register_postgres(
            name="postgres-quickstart",
            # The host name for postgres is different between Docker and Minikube
            host="host.docker.internal"
            if "docker" in custom_marks
            else "quickstart-postgres",
            port="5432",
            user="postgres",
            password="password",
            database="postgres",
            description="A Postgres deployment we created for the Featureform quickstart",
        )

        redis = ff.register_redis(
            name="redis-quickstart",
            # The host name for postgres is different between Docker and Minikube
            host="host.docker.internal"
            if "docker" in custom_marks
            else "quickstart-redis",
            port=6379,
        )

        source = provider.register_table(
            name="transactions",
            table="Transactions",  # This is the table's name in Postgres
            variant="quickstart",
        )

        return (provider, source, redis)

    return get_hosted

from behave import *
from featureform import EMRCredentials, DatabricksCredentials, SparkCredentials


@when(
    'I create EMR credentials with Cluster ID "{emr_cluster_id}", Region "{emr_cluster_region}"'
)
def step_impl(context, emr_cluster_id, emr_cluster_region):
    if emr_cluster_id == "empty":
        emr_cluster_id = ""
    if emr_cluster_region == "empty":
        emr_cluster_region = ""
    context.exception = None
    try:
        context.spark_credentials = EMRCredentials(
            emr_cluster_id=emr_cluster_id,
            emr_cluster_region=emr_cluster_region,
            credentials=context.cloud_credentials,
        )
    except Exception as e:
        context.exception = e


@when(
    'I create Databricks credentials with username "{username}", password "{password}", host "{host}", token "{token}", cluster ID "{cluster_id}"'
)
def step_impl(context, username, password, host, token, cluster_id):
    if username == "empty":
        username = ""
    if password == "empty":
        password = ""
    if host == "empty":
        host = ""
    if token == "empty":
        token = ""
    if cluster_id == "empty":
        cluster_id = ""
    context.exception = None
    try:
        context.spark_credentials = DatabricksCredentials(
            username=username,
            password=password,
            host=host,
            token=token,
            cluster_id=cluster_id,
        )
    except Exception as e:
        context.exception = e


@when(
    'I create Generic Spark credentials with master "{master}", deploy mode "{deploy_mode}", python version "{python_version}", core site path "{core_site_path}", yarn site path "{yarn_site_path}"'
)
def step_impl(
    context, master, deploy_mode, python_version, core_site_path, yarn_site_path
):
    if master == "empty":
        master = ""
    if deploy_mode == "empty":
        deploy_mode = ""
    if python_version == "empty":
        python_version = ""
    if core_site_path == "empty":
        core_site_path = ""
    if yarn_site_path == "empty":
        yarn_site_path = ""
    context.exception = None
    try:
        context.spark_credentials = SparkCredentials(
            master=master,
            deploy_mode=deploy_mode,
            python_version=python_version,
            core_site_path=core_site_path,
            yarn_site_path=yarn_site_path,
        )
    except Exception as e:
        context.exception = e

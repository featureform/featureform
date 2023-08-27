import json

from behave import *
from featureform import register_s3, register_gcs, register_blob_store


@when(
    'I register S3 with name "{name}", bucket_name "{bucket_name}", bucket_region "{bucket_region}", path "{path}"'
)
def step_impl(context, name, bucket_name, bucket_region, path):
    context.filestore_name = name
    credentials = context.cloud_credentials
    try:
        context.filestore = register_s3(
            name=name,
            bucket_name=bucket_name,
            bucket_region=bucket_region,
            path=path,
            credentials=credentials,
        )
        context.client.apply()
    except Exception as e:
        context.exception = e


@then(
    'the S3 provider should have bucket_name "{exp_bucket_name}", bucket_region "{exp_bucket_region}", path "{exp_path}", access key "{exp_access_key}" and secret key "{exp_secret_key}"'
)
def step_impl(
    context,
    exp_bucket_name,
    exp_bucket_region,
    exp_path,
    exp_access_key,
    exp_secret_key,
):
    config = context.filestore.config()
    assert (
        context.filestore.name() == context.filestore_name
    ), f"Expected name '{context.filestore_name}' but got '{context.filestore.name()}'"
    assert (
        context.filestore.store_type() == "S3"
    ), f"Expected store_type 'S3' but got '{context.filestore.store_type()}'"
    assert (
        config.bucket_name == exp_bucket_name
    ), f"Expected bucket_name '{exp_bucket_name}' but got '{config.bucket_name}'"
    assert (
        config.bucket_region == exp_bucket_region
    ), f"Expected bucket_region '{exp_bucket_region}' but got '{config.bucket_region}'"
    assert (
        config.path == exp_path
    ), f"Expected path '{exp_path}' but got '{config.path}'"
    print(exp_access_key, exp_secret_key)
    assert (
        config.credentials.access_key == exp_access_key
    ), f"Expected access_key '{exp_access_key}' but got '{config.credentials.access_key}'"
    assert (
        config.credentials.secret_key == exp_secret_key
    ), f"Expected secret_key '{exp_secret_key}' but got '{config.credentials.secret_key}'"


@when('I register GCS with name "{name}", bucket_name "{bucket_name}", path "{path}"')
def step_impl(context, name, bucket_name, path):
    context.filestore_name = name
    credentials = context.cloud_credentials
    try:
        context.filestore = register_gcs(
            name=name,
            bucket_name=bucket_name,
            path=path,
            credentials=credentials,
        )
        context.client.apply()
    except Exception as e:
        context.exception = e


@then(
    'the GCS provider should have project id "{project_id}" bucket_name "{exp_bucket_name}", path "{exp_path}", credentials from path "{exp_credentials_path}"'
)
def step_impl(context, project_id, exp_bucket_name, exp_path, exp_credentials_path):
    config = context.filestore.config()
    with open(exp_credentials_path, "r") as f:
        credentials = json.load(f)
    assert (
        context.filestore.name() == context.filestore_name
    ), f"Expected name '{context.filestore_name}' but got '{context.filestore.name()}'"
    assert (
        context.filestore.store_type() == "GCS"
    ), f"Expected store_type 'GCS' but got '{context.filestore.store_type()}'"
    assert (
        config.credentials.project_id == project_id
    ), f"Expected project_id '{project_id}' but got '{config.credentials.project_id}'"
    assert (
        config.bucket_name == exp_bucket_name
    ), f"Expected bucket_name '{exp_bucket_name}' but got '{config.bucket_name}'"
    assert (
        config.path == exp_path
    ), f"Expected path '{exp_path}' but got '{config.path}'"
    json_credentials = config.credentials.json_creds
    assert (
        json_credentials == credentials
    ), f"Expected credentials '{credentials}' but got '{json_credentials}'"


@when(
    'I register Blob Store with name "{name}", account_name "{account_name}", account_key "{account_key}", container_name "{container_name}", path "{path}"'
)
def step_impl(context, name, account_name, account_key, container_name, path):
    context.filestore_name = name
    try:
        context.filestore = register_blob_store(
            name=name,
            account_name=account_name,
            account_key=account_key,
            container_name=container_name,
            path=path,
        )
        context.client.apply()
    except Exception as e:
        print(e)
        context.exception = e


@then(
    'The blob store provider with name {name} should have account_name "{exp_account_name}", account_key "{exp_account_key}", container_name "{exp_container_name}", path "{exp_path}"'
)
def step_impl(
    context, name, exp_account_name, exp_account_key, exp_container_name, exp_path
):
    context.exception = None
    assert (
        context.filestore.name() == name
    ), f"Expected name '{name}' but got '{context.filestore.name()}'"
    assert (
        context.filestore.store_type() == "BLOB_ONLINE"
    ), f"Expected store_type 'BLOB_ONLINE' but got '{context.filestore.store_type()}'"
    # TODO: This really weird
    config = context.filestore.config().config()
    print(config)
    assert (
        config["AccountName"] == exp_account_name
    ), f"Expected account_name '{exp_account_name}' but got '{config['AccountName']}'"
    assert (
        config["AccountKey"] == exp_account_key
    ), f"Expected account_key '{exp_account_key}' but got '{config['AccountKey']}'"
    assert (
        config["ContainerName"] == exp_container_name
    ), f"Expected container_name '{exp_container_name}' but got '{config['ContainerName']}'"
    assert (
        config["Path"] == exp_path
    ), f"Expected path '{exp_path}' but got '{config['Path']}'"

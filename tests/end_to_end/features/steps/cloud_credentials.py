from behave import *
from featureform import AWSCredentials, GCPCredentials


@when('I create dummy "{cloud_provider}" credentials')
def step_impl(context, cloud_provider):
    if cloud_provider == "AWS":
        context.execute_steps(
            """
            When I create dummy AWS credentials
        """
        )
    elif cloud_provider == "GCP":
        context.execute_steps(
            """
            When I create dummy GCP credentials
        """
        )
    else:
        context.cloud_credentials = None


@when("I create dummy AWS credentials")
def step_impl(context):
    context.execute_steps(
        """
        When I create the AWS credentials with an access key "VALID_KEY" and secret key "VALID_SECRET"
    """
    )


@when(
    'I create the AWS credentials with an access key "{access_key}" and secret key "{secret_key}"'
)
def step_impl(context, access_key, secret_key):
    if access_key == "empty":
        access_key = ""
    if secret_key == "empty":
        secret_key = ""
    context.exception = None
    print(access_key, secret_key)
    try:
        context.cloud_credentials = AWSCredentials(
            access_key=access_key, secret_key=secret_key
        )
    except Exception as e:
        context.exception = e


@when("I create dummy GCP credentials")
def step_impl(context):
    context.execute_steps(
        """
       When I create the GCP credentials with a project id "dummy-project" and credentials path "test_files/dummy_creds.json"
    """
    )


@when(
    'I create the GCP credentials with a project id "{project_id}" and credentials path "{credentials_path}"'
)
def step_impl(context, project_id, credentials_path):
    if project_id == "empty":
        project_id = ""
    if credentials_path == "empty":
        credentials_path = ""
    context.exception = None
    try:
        context.cloud_credentials = GCPCredentials(
            project_id=project_id, credentials_path=credentials_path
        )
    except Exception as e:
        context.exception = e

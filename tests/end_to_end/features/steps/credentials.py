from behave import *
from featureform import AWSCredentials, GCPCredentials


@when(
    'I create the AWS credentials with an access key "{access_key}" and secret key "{secret_key}"'
)
def step_impl(context, access_key, secret_key):
    if access_key == "empty":
        access_key = ""
    if secret_key == "empty":
        secret_key = ""
    context.exception = None
    try:
        context.credentials = AWSCredentials(
            access_key=access_key, secret_key=secret_key
        )
    except Exception as e:
        context.exception = e


@when(
    'I create the GCP credentials with an project id "{project_id}" and credentials path "{credentials_path}"'
)
def step_impl(context, project_id, credentials_path):
    if project_id == "empty":
        project_id = ""
    if credentials_path == "empty":
        credentials_path = ""
    context.exception = None
    try:
        context.credentials = GCPCredentials(
            project_id=project_id, credentials_path=credentials_path
        )
    except Exception as e:
        context.exception = e


@then('An exception "{exception}" should be raised')
def step_impl(context, exception):
    if exception == "None":
        assert context.exception is None, f"Exception is {context.exception} not None"
    else:
        assert (
            str(context.exception) == exception
        ), f"Exception is: \n{context.exception} not \n{exception}"

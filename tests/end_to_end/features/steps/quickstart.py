from behave import *
import requests


@when('I pull the Docker Quickstart as "{destination}"')
def step_impl(context, destination):
    response = requests.get(
        "https://featureform-demo-files.s3.amazonaws.com/definitions.py", stream=True
    )
    with open(destination, "wb") as file:
        for chunk in response.iter_content(chunk_size=8192):
            file.write(chunk)


@then("The training set will have a label column named label")
def step_impl(context):
    last_column = context.training_set_dataframe.columns[-1]
    assert (
        last_column == "label"
    ), f"Expected last column to be label, was {last_column} instead"

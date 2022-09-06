
from behave import given


import os
import subprocess
from dotenv import load_dotenv

import featureform as ff

@given("All stuff")
def step_impl(context):
    load_dotenv("/Users/kempytoor/Documents/Featureform/repos/featureform/.env")
    output = subprocess.check_output(["featureform", "apply", "/Users/kempytoor/Documents/Featureform/playground/spark_definition.py"], text=True)
    print(output)
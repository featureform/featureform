import pytest
import featureform as ff
from featureform.resources import TrainingSet, Feature, ResourceColumnMapping
from featureform.serving import ServingClient

from datetime import timedelta


def wait_function_success(timeout=None):
    time.sleep(1)

def wait_function_fail(timeout=None):
    raise ValueError("Resource took to long to return")
    

def test_wait_feature(wait_function):
    feature = Feature(name="test_name",
                variant="test_variant")
    feature.wait_function = wait_function
    try:
        feature.wait()
    

def test_wait_label(wait_function):
    feature = Feature(name="test_name",
                variant="test_variant")
    feature.wait_function = wait_function
    try:
        feature.wait()

def test_wait_source(wait_function):
    feature = Feature(name="test_name",
                variant="test_variant")
    feature.wait_function = wait_function
    try:
        feature.wait()

def test_wait_trainingset(wait_function):
    feature = Feature(name="test_name",
                variant="test_variant")
    feature.wait_function = wait_function
    try:
        feature.wait()


def test_waits():
    wait_tests = {
        "feature": test_wait_feature,
        "training set": test_wait_trainingset,
        "source": test_wait_source,
        "label": test_wait_label
    }

    wait_functions = {
        "success": wait_function_success,
        "failure": wait_function_failure
    }

    for test in wait_tests:
        wait_tests[test](wait_functions["success"])
    for test in wait_tests:
        try:
            wait_tests[test](wait_functions["failure"])
        except ValueError as actual_error:
            actual_error.args[0] == f'Resource took to long to return'
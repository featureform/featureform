import pytest
import featureform as ff
from featureform.resources import TrainingSet, Feature, ResourceColumnMapping
from featureform.serving import ServingClient

from .test_outputs import print_outputs

from datetime import timedelta

def test_feature_print():
    feature = print_outputs["feature"]["unit_test"]["resource"]

    expected_feature_print = print_outputs["feature"]["unit_test"]["output"]

    assert expected_feature_print == f"{feature}"

    
def test_training_set_print():
    training_set = print_outputs["trainingset"]["unit_test"]["resource"]

    expected_training_set_print = print_outputs["trainingset"]["unit_test"]["output"]

    assert expected_training_set_print == f"{training_set}"
    
def test_source_print():
    source = print_outputs["source"]["unit_test"]["resource"]

    expected_source_print = print_outputs["source"]["unit_test"]["output"]

    assert expected_source_print == f"{source}"
    
def test_label_print():
    label = print_outputs["label"]["unit_test"]["resource"]

    expected_label_print = print_outputs["label"]["unit_test"]["output"]

    assert expected_label_print == f"{label}"
    
def test_entity_print():
    entity = print_outputs["entity"]["unit_test"]["resource"]

    expected_entity_print = print_outputs["entity"]["unit_test"]["output"]

    assert expected_entity_print == f"{entity}"
    
def test_user_print():
    user = print_outputs["user"]["unit_test"]["resource"]

    expected_user_print = print_outputs["user"]["unit_test"]["output"]

    assert expected_user_print == f"{user}"
    
def test_model_print():
    model = print_outputs["model"]["unit_test"]["resource"]

    expected_model_print = print_outputs["model"]["unit_test"]["output"]

    assert expected_model_print == f"{model}"
    
def test_provider_print():
    provider = print_outputs["provider"]["unit_test"]["resource"]

    expected_provider_print = print_outputs["provider"]["unit_test"]["output"]

    assert expected_provider_print == f"{provider}"


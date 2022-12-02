
import pytest
import featureform as ff
from featureform.resources import TrainingSet, Feature, ResourceColumnMapping
from featureform.serving import ServingClient

from .test_outputs import print_outputs

from datetime import timedelta

rc = ResourceClient("localhost:8000", False, False, './tls.crt')

def check_print_return(expected_print, expected_return, resource_type):
  rc_get_functions = {
    "provider": [rc.get_provider, "redis-quickstart"],
    "entity": [rc.get_entity, "user"],
    "feature": [rc.get_feature, "avg_transactions"],
    "label": [rc.get_label, "fraudulent"],
    "user": [rc.get_user, "featureformer"],
    "source": [rc.get_source, "transactions"],
    "training_set": [rc.get_training_set, "fraud_training"]
  }

  rc_get_functions_var = {
    "source_var": [rc.get_source, "transactions", "kaggle"],
    "feature_var": [rc.get_feature, "avg_transactions", "quickstart"],
    "label_var": [rc.get_label, "fraudulent", "quickstart"],
    "training_set_var": [rc.get_training_set, "fraud_training", "quickstart"]
  }

  with patch('sys.stdout', new = StringIO()) as fake_out:
    if resource_type in rc_get_functions:
      value = rc_get_functions[resource_type][0](rc_get_functions[resource_type][1])
    if resource_type in rc_get_functions_var:
      value = rc_get_functions_var[resource_type][0](rc_get_functions_var[resource_type][1], rc_get_functions_var[resource_type][2])
    assert (fake_out.getvalue().replace(" ","") == expected_print.replace(" ", ""))
  assert re.match(expected_return.replace(" ", ""), str(value).replace(" ", ""))

def test_get_provider():
  expected_print_provider = print_outputs["provider"]["e2e_test"]["output"]
  expected_return_provider = print_outputs["provider"]["e2e_test"]["return"]
  check_print_return(expected_print_provider, expected_return_provider, "provider")
  

def test_get_entity():
  expected_print_entity = print_outputs["entity"]["e2e_test"]["output"]
  expected_return_entity = print_outputs["entity"]["e2e_test"]["return"]
  check_print_return(expected_print_entity, expected_return_entity, "entity")

def test_get_user():
  expected_print_user = print_outputs["user"]["e2e_test"]["output"]
  expected_return_user = print_outputs["user"]["e2e_test"]["return"]
  check_print_return(expected_print_user, expected_return_user, "user")

def test_get_source():
  expected_print_source = print_outputs["source"]["e2e_test"]["resource"]["output"]
  expected_return_source = print_outputs["source"]["e2e_test"]["resource"]["return"]
  check_print_return(expected_print_source, expected_return_source, "source")

def test_get_source_var():
  expected_print_source_var = print_outputs["source"]["e2e_test"]["variant"]["output"]
  expected_return_source_var = print_outputs["source"]["e2e_test"]["variant"]["return"]
  check_print_return(expected_print_source_var, expected_return_source_var, "source_var")

def test_get_feature():
  expected_print_feature = print_outputs["feature"]["e2e_test"]["resource"]["output"]
  expected_return_feature = print_outputs["feature"]["e2e_test"]["resource"]["return"]
  check_print_return(expected_print_feature, expected_return_feature, "feature")

def test_get_feature_var():
  expected_print_feature_var = print_outputs["feature"]["e2e_test"]["variant"]["output"]
  expected_return_feature_var = print_outputs["feature"]["e2e_test"]["variant"]["return"]
  check_print_return(expected_print_feature_var, expected_return_feature_var, "feature_var")

def test_get_label():
  expected_print_label = print_outputs["label"]["e2e_test"]["resource"]["output"]
  expected_return_label = print_outputs["label"]["e2e_test"]["resource"]["return"]
  check_print_return(expected_print_label, expected_return_label, "label")

def test_get_label_var():
  expected_print_label_var = print_outputs["label"]["e2e_test"]["variant"]["output"]
  expected_return_label_var = print_outputs["label"]["e2e_test"]["variant"]["return"]
  check_print_return(expected_print_label_var, expected_return_label_var, "label_var")

def test_get_training_set():
  expected_print_training_set = print_outputs["trainingset"]["e2e_test"]["resource"]["output"]
  expected_return_training_set = print_outputs["trainingset"]["e2e_test"]["resource"]["return"]
  check_print_return(expected_print_training_set, expected_return_training_set, "training_set")

def test_get_training_set_var():
  expected_print_training_set_var = print_outputs["trainingset"]["e2e_test"]["variant"]["output"]
  expected_return_training_set_var = print_outputs["trainingset"]["e2e_test"]["variant"]["return"]
  check_print_return(expected_print_training_set_var, expected_return_training_set_var, "training_set_var")

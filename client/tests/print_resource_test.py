import pytest
import featureform as ff
from featureform.resources import TrainingSet, Feature, ResourceColumnMapping
from featureform.serving import ServingClient

from .test_outputs import print_outputs

from datetime import timedelta


@pytest.fixture
def feature_object():
      return print_outputs["feature"]["unit_test"]["object"]

@pytest.fixture
def feature_expected_print():
      return print_outputs["feature"]["unit_test"]["output"]

def test_feature_print(feature_object, feature_expected_print):
       assert feature_expected_print == f"{feature_object}"

#Training set
@pytest.fixture
def training_set_object():
      return print_outputs["trainingset"]["unit_test"]["object"]

@pytest.fixture
def training_set_expected_print():
      return print_outputs["trainingset"]["unit_test"]["output"]

def test_training_set_print(training_set_object, training_set_expected_print):
       assert training_set_expected_print == f"{training_set_object}"

#Label

@pytest.fixture
def label_object():
      return print_outputs["label"]["unit_test"]["object"]

@pytest.fixture
def label_expected_print():
      return print_outputs["label"]["unit_test"]["output"]

def test_label_print(label_object, label_expected_print):
       assert label_expected_print == f"{label_object}"

#Source

@pytest.fixture
def source_object():
      return print_outputs["source"]["unit_test"]["object"]

@pytest.fixture
def source_expected_print():
      return print_outputs["source"]["unit_test"]["output"]

def test_source_print(source_object, source_expected_print):
       assert label_expected_print == f"{source_object}"

#User

@pytest.fixture
def user_object():
      return print_outputs["user"]["unit_test"]["object"]

@pytest.fixture
def user_expected_print():
      return print_outputs["user"]["unit_test"]["output"]

def test_user_print(user_object, user_expected_print):
       assert user_expected_print == f"{user_object}"

#Entity

@pytest.fixture
def entity_object():
      return print_outputs["entity"]["unit_test"]["object"]

@pytest.fixture
def entity_expected_print():
      return print_outputs["entity"]["unit_test"]["output"]

def test_entity_print(entity_object, entity_expected_print):
       assert entity_expected_print == f"{entity_object}"

#Model

@pytest.fixture
def model_object():
      return print_outputs["model"]["unit_test"]["object"]

@pytest.fixture
def model_expected_print():
      return print_outputs["model"]["unit_test"]["output"]

def test_model_print(model_object, model_expected_print):
       assert model_expected_print == f"{model_object}"

#Provider

@pytest.fixture
def provider_object():
      return print_outputs["provider"]["unit_test"]["object"]

@pytest.fixture
def provider_expected_print():
      return print_outputs["provider"]["unit_test"]["output"]

def test_provider_print(provider_object, provider_expected_print):
       assert provider_expected_print == f"{provider_object}"


import pytest
import featureform as ff
from featureform.resources import TrainingSet, Feature, ResourceColumnMapping
from featureform.serving import ServingClient

from datetime import timedelta


TRAININGSET_NAME = "test_training_set"
TRAININGSET_VARIANT = "test_ts_variant"

FEATURE_NAME = "test_feature"
FEATURE_VARIANT = "test_feature_variant"

FEATURE_ENTITY = "test_entity"
FEATURE_VALUE = "test_value"

def test_not_ready_timeout_training_set(mocker):
    mocker.patch(
        'featureform.register.ResourceClient.get_training_set',
        return_value=TrainingSet(
            name="",
            owner="",
            label=("test","variant"),
            features=[("test","variant")],
            feature_lags=[],
            description="",
            status="PENDING"
        )
    )
    client=ServingClient(host="mock_host")
    try:
        training_set = dataset = client.training_set(TRAININGSET_NAME, TRAININGSET_VARIANT).wait(timeout=2)
    except ValueError as actual_error:
        assert actual_error.args[0] == f'Waited too long for resource {TRAININGSET_NAME}:{TRAININGSET_VARIANT} to be ready'

def test_failed_training_set(mocker):
    mocker.patch(
        'featureform.register.ResourceClient.get_training_set',
        return_value=TrainingSet(
            name="",
            owner="",
            label=("test","variant"),
            features=[("test","variant")],
            feature_lags=[],
            description="",
            status="FAILED"
        )
    )
    client=ServingClient(host="mock_host")
    try:
        dataset = client.training_set(TRAININGSET_NAME, TRAININGSET_VARIANT).wait()
    except ValueError as actual_error:
        assert actual_error.args[0] == f'Resource {TRAININGSET_NAME}:{TRAININGSET_VARIANT} status set to failed while waiting'

def test_ready_training_set(mocker):
    mocker.patch(
        'featureform.register.ResourceClient.get_training_set',
        return_value=TrainingSet(
            name="",
            owner="",
            label=("test","variant"),
            features=[("test","variant")],
            feature_lags=[],
            description="",
            status="READY"
        )
    )

    client=ServingClient(host="mock_host")
    actual = client.training_set(TRAININGSET_NAME, TRAININGSET_VARIANT).wait()
    assert actual != None, "Training set get returned None"

def test_timeout_feature(mocker):
    mocker.patch(
        'featureform.register.ResourceClient.get_feature',
        return_value=Feature(
            name="",
            source=("",""),
            entity="",
            value_type="",
            owner="",
            provider="",
            location=ResourceColumnMapping(entity="",value="",timestamp=""),
            description="",
            status="PENDING"
        )
    )
    client=ServingClient(host="mock_host")
    try:
        features = client.features([(FEATURE_NAME, FEATURE_VARIANT)], {FEATURE_ENTITY: FEATURE_VALUE}).wait(timeout=2)
    except ValueError as actual_error:
        assert actual_error.args[0] == f'Waited too long for resource {FEATURE_NAME}:{FEATURE_VARIANT} to be ready'

def test_failed_feature(mocker):
    mocker.patch(
        'featureform.register.ResourceClient.get_feature',
        return_value=Feature(
            name="",
            source=("",""),
            entity="",
            value_type="",
            owner="",
            provider="",
            location=ResourceColumnMapping(entity="",value="",timestamp=""),
            description="",
            status="FAILED"
        )
    )
    client=ServingClient(host="mock_host")
    try:
        features = client.features([(FEATURE_NAME, FEATURE_VARIANT)], {FEATURE_ENTITY: FEATURE_VALUE}).wait(timeout=2)
    except ValueError as actual_error:
        assert actual_error.args[0] == f'Resource {FEATURE_NAME}:{FEATURE_VARIANT} status set to failed while waiting'

def test_ready_feature(mocker):
    mocker.patch(
        'featureform.register.ResourceClient.get_feature',
        return_value=Feature(
            name="",
            source=("",""),
            entity="",
            owner="",
            value_type="",
            provider="",
            location=ResourceColumnMapping(entity="",value="",timestamp=""),
            description="",
            status="READY"
        )
    )
    mocker.patch(
        'featureform.serving.FeatureServer._get_features',
        return_value=""
    )

    expected = Feature(
            name="",
            source=("",""),
            entity="",
            value_type="",
            owner="",
            provider="",
            location=ResourceColumnMapping(entity="",value="",timestamp=""),
            description="",
            status="READY"
        )
    client=ServingClient(host="mock_host")
    actual = client.features([(FEATURE_NAME, FEATURE_VARIANT)], {FEATURE_ENTITY: FEATURE_VALUE}).wait(timeout=2)
    assert actual != None, "Feature get returned None"


import pytest

from datetime import timedelta


TRAININGSET_NAME = "test_training_set"
TRAININGSET_VARIANT = "test_ts_variant"

FEATURE_NAME = "test_feature"
FEATURE_VARIANT = "test_feature_variant"



#Training set

# not ready, expect timeout
def test_not_ready_timeout_training_set(mocker):
    mocker.patch(
        # api_call is from slow.py but imported to main.py
        'featureform.register.get_training_set',
        return_value=TrainingSet(
            name="",
            owner="",
            label=("",""),
            features=[("","")],
            feature_lags=[],
            description="",
            status="PENDING"
        )
    )

    try:
        training_set = dataset = client.training_set(TRAININGSET_NAME, TRAININGSET_VARIANT).wait(timeout=2)
    except ValueError as actual_error:
        assert actual_error.message == "Waited too long for resource test_training_set:test_ts_variant to be ready"


#failed expect error

def test_failed_training_set(mocker):
    mocker.patch(
        # api_call is from slow.py but imported to main.py
        'featureform.register.get_training_set',
        return_value=TrainingSet(
            name="",
            owner="",
            label=("",""),
            features=[("","")],
            feature_lags=[],
            description="",
            status="FAILED"
        )
    )
    try:
        dataset = client.training_set(TRAININGSET_NAME, TRAININGSET_VARIANT).wait()
    except ValueError as actual_error:
        assert actual_error.message == "Resource test_training_set:test_ts_variant status set to failed while waiting"

def test_ready_training_set(mocker):
    mocker.patch(
        # api_call is from slow.py but imported to main.py
        'featureform.register.get_training_set',
        return_value=TrainingSet(
            name="",
            owner="",
            label=("",""),
            features=[("","")],
            feature_lags=[],
            description="",
            status="READY"
        )
    )

    expected = TrainingSet(
            name="",
            owner="",
            label=("",""),
            features=[("","")],
            feature_lags=[],
            description="",
            status="READY"
        )
    actual = dataset = client.training_set(TRAININGSET_NAME, TRAININGSET_VARIANT).wait()
    assert expected == actual

#Feature

def test_timeout_feature(mocker):
    mocker.patch(
        # api_call is from slow.py but imported to main.py
        'featureform.register.get_feature',
        return_value=Feature(
            name="",
            source=("",""),
            entity="",
            owner="",
            provider="",
            location=None,
            description="",
            status="PENDING"
        )
    )
    try:

        features = client.features([(FEATURE_NAME, FEATURE_VARIANT)], {FEATURE_ENTITY: FEATURE_VALUE}).wait(timeout=2)
    except ValueError as actual_error:
        assert actual_error.message == "Waited too long for resource test_feature:test_feature_variant to be ready"

#failed expect error

def test_failed_feature(mocker):
    mocker.patch(
        # api_call is from slow.py but imported to main.py
        'featureform.register.get_feature',
        return_value=Feature(
            name="",
            source=("",""),
            entity="",
            owner="",
            provider="",
            location=None,
            description="",
            status="FAILED"
        )
    )
    try:
        features = client.features([(FEATURE_NAME, FEATURE_VARIANT)], {FEATURE_ENTITY: FEATURE_VALUE}).wait(timeout=2)
    except ValueError as actual_error:
        assert actual_error.message == "Resource test_feature:test_feature_variant status set to failed while waiting"

def test_ready_feature(mocker):
    mocker.patch(
        # api_call is from slow.py but imported to main.py
        'featureform.register.get_feature',
        return_value=Feature(
            name="",
            source=("",""),
            entity="",
            owner="",
            provider="",
            location=None,
            description="",
            status="READY"
        )
    )

    expected = Feature(
            name="",
            source=("",""),
            entity="",
            owner="",
            provider="",
            location=None,
            description="",
            status="READY"
        )
    actual = client.features([(FEATURE_NAME, FEATURE_VARIANT)], {FEATURE_ENTITY: FEATURE_VALUE}).wait(timeout=2)
    assert expected == actual

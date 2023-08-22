import featureform as ff
from featureform.type_objects import (
    FeatureVariantResource,
    FeatureResource,
    TrainingSetVariantResource,
    ModelResource,
    ProviderResource,
)
import os
import pytest
import time
from datetime import datetime
import json


def test_featurevariantresource():
    resource = FeatureVariantResource(
        created="time",
        description="Description",
        entity="entity",
        name="name",
        owner="owner",
        provider="provider",
        dataType="type",
        variant="variant",
        status="status",
        location=None,
        source=None,
        tags=["tag1", "tag2"],
        properties={"key": "value"},
    )
    dictionary = resource.to_dict()
    assert dictionary == {
        "created": "time",
        "description": "Description",
        "entity": "entity",
        "name": "name",
        "owner": "owner",
        "provider": "provider",
        "data-type": "type",
        "variant": "variant",
        "status": "status",
        "location": None,
        "source": None,
        "tags": ["tag1", "tag2"],
        "properties": {"key": "value"},
    }
    assert resource.serialize() == json.dumps(dictionary)


def test_feature_resource():
    resource = FeatureResource(
        name="feature_name",
        default_variant="default_variant",
        type="type",
        variants=None,
        all_variants=[],
    )
    dictionary = resource.to_dict()
    assert dictionary == {
        "name": "feature_name",
        "defaultVariant": "default_variant",
        "type": "type",
        "variants": None,
        "allVariants": [],
    }
    assert resource.serialize() == json.dumps(dictionary)


def test_trainingset_variant_resource():
    resource = TrainingSetVariantResource(
        created="time",
        description="Description",
        name="name",
        owner="owner",
        variant="variant",
        label=None,
        status="status",
        features=None,
        tags=["tag1", "tag2"],
        properties={"key": "value"},
    )
    dictionary = resource.to_dict()
    assert dictionary == {
        "created": "time",
        "description": "Description",
        "name": "name",
        "owner": "owner",
        "variant": "variant",
        "label": None,
        "status": "status",
        "features": None,
        "tags": ["tag1", "tag2"],
        "properties": {"key": "value"},
    }
    assert resource.serialize() == json.dumps(dictionary)


def test_model_resource():
    resource = ModelResource(
        name="model_name",
        type="type",
        description="Description",
        status="status",
        features=None,
        labels=None,
        training_sets=None,
        tags=["tag1", "tag2"],
        properties={"key": "value"},
    )
    dictionary = resource.to_dict()
    assert dictionary == {
        "name": "model_name",
        "type": "type",
        "description": "Description",
        "status": "status",
        "features": None,
        "labels": None,
        "trainingSets": None,
        "tags": ["tag1", "tag2"],
        "properties": {"key": "value"},
    }
    assert resource.serialize() == json.dumps(dictionary)


def test_provider_resource():
    resource = ProviderResource(
        name="provider_name",
        type="type",
        description="Description",
        provider_type="provider_type",
        software="software",
        team="team",
        sources=None,
        status="status",
        serialized_config="config",
        features=None,
        labels=None,
        tags=["tag1", "tag2"],
        properties={"key": "value"},
    )
    dictionary = resource.to_dict()
    assert dictionary == {
        "name": "provider_name",
        "type": "type",
        "description": "Description",
        "provider-type": "provider_type",
        "software": "software",
        "team": "team",
        "sources": None,
        "status": "status",
        "serializedConfig": "config",
        "features": None,
        "labels": None,
        "tags": ["tag1", "tag2"],
        "properties": {"key": "value"},
    }
    assert resource.serialize() == json.dumps(dictionary)

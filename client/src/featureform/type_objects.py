from datetime import datetime
from ssl import create_default_context
import json


class FeatureVariantResource:
    def __init__(
        self,
        created=None,
        description="",
        entity="",
        name="",
        owner="",
        provider="",
        dataType="",
        variant="",
        status="",
        location=None,
        source=None,
        tags=[],
        properties={},
    ):
        self.__dictionary = {
            "created": created,
            "description": description,
            "entity": entity,
            "name": name,
            "owner": owner,
            "provider": provider,
            "data-type": dataType,
            "variant": variant,
            "status": status,
            "location": location,
            "source": source,
            "tags": tags,
            "properties": properties,
            # Training Set[] is missing
        }

    def to_dict(self):
        return self.__dictionary

    def serialize(self):
        return json.dumps(self.__dictionary)


class FeatureResource:
    def __init__(
        self, name="", default_variant="", type="", variants=None, all_variants=[]
    ):
        self.__dictionary = {
            "all-variants": all_variants,
            "type": type,
            "default-variant": default_variant,
            "name": name,
            "variants": variants,
        }

    def to_dict(self):
        return self.__dictionary

    def serialize(self):
        return json.dumps(self.__dictionary)


class TrainingSetVariantResource:
    def __init__(
        self,
        created=None,
        description="",
        name="",
        owner="",
        variant="",
        label=None,
        status="",
        features=None,
        tags=[],
        properties={},
    ):
        self.__dictionary = {
            "created": created,
            "description": description,
            "name": name,
            "owner": owner,
            "variant": variant,
            "status": status,
            "label": label,
            "features": features,
            "tags": tags,
            "properties": properties,
        }

    def to_dict(self):
        return self.__dictionary

    def serialize(self):
        return json.dumps(self.__dictionary)


class TrainingSetResource:
    def __init__(
        self, type="", default_variant="", name="", variants=None, all_variants=[]
    ):
        self.__dictionary = {
            "all-variants": all_variants,
            "type": type,
            "default-variant": default_variant,
            "name": name,
            "variants": variants,
        }

    def to_dict(self):
        return self.__dictionary

    def serialize(self):
        return json.dumps(self.__dictionary)


class SourceVariantResource:
    def __init__(
        self,
        created=None,
        description="",
        name="",
        source_type="",
        owner="",
        provider="",
        variant="",
        status="",
        definition="",
        labels=None,
        features=None,
        training_sets=None,
        tags=[],
        properties={},
    ):
        self.__dictionary = {
            "created": created,
            "description": description,
            "name": name,
            "source-type": source_type,
            "owner": owner,
            "provider": provider,
            "variant": variant,
            "status": status,
            "definition": definition,
            "labels": labels,
            "features": features,
            "trainingSets": training_sets,
            "tags": tags,
            "properties": properties,
        }

    def to_dict(self):
        return self.__dictionary

    def serialize(self):
        return json.dumps(self.__dictionary)

    @property
    def name(self):
        return self.__dictionary["name"]

    @property
    def variant(self):
        return self.__dictionary["variant"]


class SourceResource:
    def __init__(
        self, type="", default_variant="", name="", variants=None, all_variants=[]
    ):
        self.__dictionary = {
            "all-variants": default_variant,
            "type": type,
            "default-variant": all_variants,
            "name": name,
            "variants": variants,
        }

    def to_dict(self):
        return self.__dictionary

    def serialize(self):
        return json.dumps(self.__dictionary)


class LabelVariantResource:
    def __init__(
        self,
        created=None,
        description="",
        entity="",
        name="",
        owner="",
        provider="",
        dataType="",
        variant="",
        location=None,
        status="",
        source=None,
        training_sets=None,
        tags=[],
        properties={},
    ):
        self.__dictionary = {
            "created": created,
            "description": description,
            "entity": entity,
            "data-type": dataType,
            "name": name,
            "owner": owner,
            "provider": provider,
            "variant": variant,
            "status": status,
            "location": location,
            "source": source,
            "trainingSets": training_sets,
            "tags": tags,
            "properties": properties,
            #  source is missing
        }

    def to_dict(self):
        return self.__dictionary

    def serialize(self):
        return json.dumps(self.__dictionary)


class LabelResource:
    def __init__(
        self, type="", default_variant="", name="", variants=None, all_variants=[]
    ):
        self.__dictionary = {
            "all-variants": default_variant,
            "type": type,
            "default-variant": all_variants,
            "name": name,
            "variants": variants,
        }

    def to_dict(self):
        return self.__dictionary

    def serialize(self):
        return json.dumps(self.__dictionary)


class EntityResource:
    def __init__(
        self,
        name="",
        type="",
        description="",
        status="",
        features=None,
        labels=None,
        training_sets=None,
        tags=[],
        properties={},
    ):
        self.__dictionary = {
            "description": description,
            "type": type,
            "name": name,
            "features": features,
            "labels": labels,
            "training-sets": training_sets,
            "status": status,
            "tags": tags,
            "properties": properties,
        }

    def to_dict(self):
        return self.__dictionary

    def serialize(self):
        return json.dumps(self.__dictionary)


class UserResource:
    def __init__(
        self,
        name="",
        type="",
        status="",
        features=None,
        labels=None,
        training_sets=None,
        sources=None,
        tags=[],
        properties={},
    ):
        self.__dictionary = {
            "name": name,
            "type": type,
            "features": features,
            "labels": labels,
            "training-sets": training_sets,
            "sources": sources,
            "status": status,
            "tags": tags,
            "properties": properties,
        }

    def to_dict(self):
        return self.__dictionary

    def serialize(self):
        return json.dumps(self.__dictionary)


class ModelResource:
    def __init__(
        self,
        name="",
        type="",
        description="",
        status="",
        features=None,
        labels=None,
        training_sets=None,
        tags=[],
        properties={},
    ):
        self.__dictionary = {
            "name": name,
            "type": type,
            "description": description,
            "features": features,
            "labels": labels,
            "trainingSets": training_sets,
            "status": status,
            "tags": tags,
            "properties": properties,
        }

    def to_dict(self):
        return self.__dictionary

    def serialize(self):
        return json.dumps(self.__dictionary)


class ProviderResource:
    def __init__(
        self,
        name="",
        type="",
        description="",
        provider_type="",
        software="",
        team="",
        sources=None,
        status="",
        serialized_config="",
        features=None,
        labels=None,
        tags=[],
        properties={}
        # trainingSets=None
    ):
        self.__dictionary = {
            "name": name,
            "type": type,
            "description": description,
            "provider-type": provider_type,
            "software": software,
            "team": team,
            "sources": sources,
            "features": features,
            "labels": labels,
            # "training-sets":trainingSets,
            "status": status,
            #   Seems like we dont need serialised config
            "serializedConfig": serialized_config,
            "tags": tags,
            "properties": properties,
        }

    def to_dict(self):
        return self.__dictionary

    def serialize(self):
        return json.dumps(self.__dictionary)

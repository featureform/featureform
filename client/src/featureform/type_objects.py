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
        definition="",
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
            "definition": definition,
        }

    def to_dictionary(self):
        return self.__dictionary

    def to_json_literal(self):
        return json.dumps(self.__dictionary)


class FeatureResource:
    def __init__(
        self, name="", defaultVariant="", type="", variants=None, allVariants=[]
    ):
        self.__dictionary = {
            "all-variants": allVariants,
            "type": type,
            "default-variant": defaultVariant,
            "name": name,
            "variants": variants,
        }

    def to_dictionary(self):
        return self.__dictionary

    def to_json_literal(self):
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

    def to_dictionary(self):
        return self.__dictionary

    def to_json_literal(self):
        return json.dumps(self.__dictionary)


class TrainingSetResource:
    def __init__(
        self, type="", defaultVariant="", name="", variants=None, allVariants=[]
    ):
        self.__dictionary = {
            "all-variants": allVariants,
            "type": type,
            "default-variant": defaultVariant,
            "name": name,
            "variants": variants,
        }

    def to_dictionary(self):
        return self.__dictionary

    def to_json_literal(self):
        return json.dumps(self.__dictionary)


class SourceVariantResource:
    def __init__(
        self,
        created=None,
        description="",
        name="",
        sourceType="",
        owner="",
        provider="",
        variant="",
        status="",
        definition="",
        labels=None,
        features=None,
        trainingSets=None,
        tags=[],
        properties={},
        inputs=[],
    ):
        self.__dictionary = {
            "created": created,
            "description": description,
            "name": name,
            "source-type": sourceType,
            "owner": owner,
            "provider": provider,
            "variant": variant,
            "status": status,
            "definition": definition,
            "labels": labels,
            "features": features,
            "training-sets": trainingSets,
            "tags": tags,
            "properties": properties,
            "inputs": inputs,
        }

    def to_dictionary(self):
        return self.__dictionary

    def to_json_literal(self):
        return json.dumps(self.__dictionary)

    @property
    def name(self):
        return self.__dictionary["name"]

    @property
    def variant(self):
        return self.__dictionary["variant"]


class SourceResource:
    def __init__(
        self, type="", defaultVariant="", name="", variants=None, allVariants=[]
    ):
        self.__dictionary = {
            "all-variants": allVariants,
            "type": type,
            "default-variant": defaultVariant,
            "name": name,
            "variants": variants,
        }

    def to_dictionary(self):
        return self.__dictionary

    def to_json_literal(self):
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
        trainingSets=None,
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
            "training-sets": trainingSets,
            "tags": tags,
            "properties": properties,
            #  source is missing
        }

    def to_dictionary(self):
        return self.__dictionary

    def to_json_literal(self):
        return json.dumps(self.__dictionary)


class LabelResource:
    def __init__(
        self, type="", defaultVariant="", name="", variants=None, allVariants=[]
    ):
        self.__dictionary = {
            "all-variants": allVariants,
            "type": type,
            "default-variant": defaultVariant,
            "name": name,
            "variants": variants,
        }

    def to_dictionary(self):
        return self.__dictionary

    def to_json_literal(self):
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
        trainingSets=None,
        tags=[],
        properties={},
    ):
        self.__dictionary = {
            "description": description,
            "type": type,
            "name": name,
            "features": features,
            "labels": labels,
            "training-sets": trainingSets,
            "status": status,
            "tags": tags,
            "properties": properties,
        }

    def to_dictionary(self):
        return self.__dictionary

    def to_json_literal(self):
        return json.dumps(self.__dictionary)


class UserResource:
    def __init__(
        self,
        name="",
        type="",
        status="",
        features=None,
        labels=None,
        trainingSets=None,
        sources=None,
        tags=[],
        properties={},
    ):
        self.__dictionary = {
            "name": name,
            "type": type,
            "features": features,
            "labels": labels,
            "training-sets": trainingSets,
            "sources": sources,
            "status": status,
            "tags": tags,
            "properties": properties,
        }

    def to_dictionary(self):
        return self.__dictionary

    def to_json_literal(self):
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
        trainingSets=None,
        tags=[],
        properties={},
    ):
        self.__dictionary = {
            "name": name,
            "type": type,
            "description": description,
            "features": features,
            "labels": labels,
            "training-sets": trainingSets,
            "status": status,
            "tags": tags,
            "properties": properties,
        }

    def to_dictionary(self):
        return self.__dictionary

    def to_json_literal(self):
        return json.dumps(self.__dictionary)


class ProviderResource:
    def __init__(
        self,
        name="",
        type="",
        description="",
        providerType="",
        software="",
        team="",
        sources=None,
        status="",
        serializedConfig="{}",
        features=None,
        labels=None,
        tags=[],
        properties={},
    ):
        self.__dictionary = {
            "name": name,
            "type": type,
            "description": description,
            "provider-type": providerType,
            "software": software,
            "team": team,
            "sources": sources,
            "features": features,
            "labels": labels,
            "status": status,
            #   Seems like we dont need serialised config
            "serializedConfig": serializedConfig,
            "tags": tags,
            "properties": properties,
        }

    def to_dictionary(self):
        return self.__dictionary

    def to_json_literal(self):
        return json.dumps(self.__dictionary)

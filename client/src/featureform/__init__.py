from .register import *
from .serving import ServingClient
from .resources import (
    DatabricksCredentials,
    EMRCredentials,
    AWSCredentials,
    GCPCredentials,
    SparkCredentials,
    ColumnTypes,
)
from typing import Union, Dict

ServingClient = ServingClient
ResourceClient = ResourceClient


def entity(cls, name=None, lowercase=False):
    # 1. Use provided name or take the name of the class as the entity name,
    #    adjust the casing if necessary, then register it
    if name is None:
        name = cls.__name__
    if lowercase:
        name = name.lower()
    entity = register_entity(name)
    # 2. Given the Feature/Label/Variant class constructors are evaluated
    #    before the entity decorator, apply the entity name to their
    #    respective dictionaries
    for attr_name in cls.__dict__:
        # Refactor the following to avoid repetition
        if isinstance(cls.__dict__[attr_name], (Feature, Label)):
            resource = cls.__dict__[attr_name]
            resource.name = attr_name
            resource.entity = entity
            resource.register_resources()
        elif isinstance(cls.__dict__[attr_name], Variants):
            variants = cls.__dict__[attr_name]
            for variant_key, resource in variants.resources.items():
                resource.name = attr_name
                resource.entity = entity
                resource.register_resources()
    return cls


class Feature:
    def __init__(
        self,
        transformation_args: tuple,
        type: Union[ColumnTypes, str],
        entity: Union[Entity, str] = "",
        variant="default",
        inference_store: Union[str, OnlineProvider, FileStoreProvider] = "",
    ):
        registrar, source_name_variant, columns = transformation_args
        self.type = type if isinstance(type, str) else type.value
        self.registrar = registrar
        self.source = source_name_variant
        self.entity_column = columns[0]
        self.source_column = columns[1]
        self.entity = entity
        self.variant = variant
        self.inference_store = inference_store

    def register_resources(self):
        self.registrar.register_column_resources(
            source=self.source,
            entity=self.entity,
            entity_column=self.entity_column,
            owner="",
            inference_store=self.inference_store,
            features=[
                {
                    "name": self.name,
                    "variant": self.variant,
                    "column": self.source_column,
                    "type": self.type,
                }
            ],
            labels=[],
            timestamp_column="",
            description="",
            schedule="",
        )


class Label:
    def __init__(
        self,
        transformation_args: tuple,
        type: str,
        entity: Union[Entity, str] = "",
        variant="default",
        inference_store: Union[str, OnlineProvider, FileStoreProvider] = "",
    ):
        registrar, source_name_variant, columns = transformation_args
        self.type = type if isinstance(type, str) else type.value
        self.registrar = registrar
        self.source = source_name_variant
        self.entity_column = columns[0]
        self.source_column = columns[1]
        self.entity = entity
        self.variant = variant
        self.inference_store = inference_store

    def register_resources(self):
        self.registrar.register_column_resources(
            source=self.source,
            entity=self.entity,
            entity_column=self.entity_column,
            owner="",
            inference_store=self.inference_store,
            features=[],
            labels=[
                {
                    "name": self.name,
                    "variant": self.variant,
                    "column": self.source_column,
                    "type": self.type,
                }
            ],
            timestamp_column="",
            description="",
            schedule="",
        )


class Variants:
    def __init__(self, resources: Dict[str, Union[Feature, Label]]):
        self.resources = resources
        self.validate_variant_names()

    def validate_variant_names(self):
        for variant_key, resource in self.resources.items():
            if resource.variant != variant_key:
                raise ValueError(
                    f"Variant name {variant_key} does not match resource variant name {resource.variant}"
                )

    def register_resources(self):
        for resource in self.resources.values():
            resource.register_resources()


# Executor Credentials
DatabricksCredentials = DatabricksCredentials
EMRCredentials = EMRCredentials
SparkCredentials = SparkCredentials

# Cloud Provider Credentials
AWSCredentials = AWSCredentials
GCPCredentials = GCPCredentials

local = register_local()
register_user("default_user").make_default_owner()

Feature = Feature
Label = Label
Variants = Variants

Float32 = ColumnTypes.FLOAT32

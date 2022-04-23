# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

from typing import List, Tuple
from typeguard import typechecked


class RedisConfig:

    @typechecked
    def __init__(self, host: str, port: int, password: str, db: int):
        pass

    @typechecked
    def software(self) -> str:
        return "redis"


class SnowflakeConfig:

    @typechecked
    def __init__(self, account: str, database: str, organization: str,
                 username: str, password: str, schema: str):
        self.account = account
        self.database = database
        self.organization = organization
        self.username = username
        self.password = password
        self.schema = schema

    @typechecked
    def software(self) -> str:
        return "snowflake"


class PostgresConfig:

    @typechecked
    def __init__(self, host: str, port: int, database: str, user: str,
                 password: str):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password

    @typechecked
    def software(self) -> str:
        return "postgres"


Config = RedisConfig | SnowflakeConfig | PostgresConfig


class Provider:

    @typechecked
    def __init__(self,
                 name: str,
                 function: str,
                 config: Config,
                 description: str = "",
                 team: str = ""):
        self.name = name
        self.description = description
        self.function = function
        self.software = config.software()
        self.team = team
        self.config = config

    @staticmethod
    def type() -> str:
        return "provider"


class User:

    @typechecked
    def __init__(self, name: str):
        self.name = name

    @typechecked
    def type(self) -> str:
        return "user"


class PrimaryData:

    @typechecked
    def __init__(self,
                 name: str,
                 variant: str,
                 t: str,
                 owner: str,
                 provider: str,
                 description: str = ""):
        self.name = name
        self.variant = variant
        self.t = t
        self.owner = owner
        self.description = description
        self.provider = provider

    @staticmethod
    def type() -> str:
        return "source"


class Entity:

    @typechecked
    def __init__(self, name: str, description: str):
        self.name = name
        self.description = description

    @staticmethod
    def type() -> str:
        return "entity"


class Feature:

    @typechecked
    def __init__(self,
                 name: str,
                 variant: str,
                 t: str,
                 entity: str,
                 owner: str,
                 provider: str,
                 description: str = ""):
        self.name = name
        self.variant = variant
        self.description = description
        self.t = t
        self.entity = entity
        self.owner = owner
        self.provider = provider

    @staticmethod
    def type() -> str:
        return "feature"


class Label:

    @typechecked
    def __init__(self,
                 name: str,
                 variant: str,
                 t: str,
                 entity: str,
                 owner: str,
                 provider: str,
                 description: str = ""):
        self.name = name
        self.variant = variant
        self.description = description
        self.t = t
        self.entity = entity
        self.owner = owner
        self.provider = provider

    @staticmethod
    def type() -> str:
        return "label"


NameVariant = Tuple[str, str]


class TrainingSet:

    @typechecked
    def __init__(self,
                 name: str,
                 variant: str,
                 owner: str,
                 provider: str,
                 label: NameVariant,
                 features: List[NameVariant],
                 description: str = ""):
        valid_name_variant = lambda t: t[0] != "" and t[1] != ""
        if not valid_name_variant(label):
            raise ValueError("Label must be set")
        if len(features) == 0:
            raise ValueError("A training-set must have atleast one feature")
        for feature in features:
            if not valid_name_variant(feature):
                raise ValueError("Invalid Feature")
        self.name = name
        self.variant = variant
        self.description = description
        self.owner = owner
        self.provider = provider
        self.label = label
        self.features = features

    @staticmethod
    def type() -> str:
        return "training-set"


ResourceType = PrimaryData | Provider | Entity | User | Feature | Label | TrainingSet


class ResourceRedefinedError(Exception):

    @typechecked
    def __init__(self, resource: ResourceType):
        variantStr = f" variant {resource.variant}" if hasattr(
            resource, 'variant') else ""
        resourceId = f"{resource.name}{variantStr}"
        super().__init__(
            f"{resource.type()} resource {resourceId} defined in multiple places"
        )


class ResourceState:

    def __init__(self):
        self.__state = {}

    @typechecked
    def add(self, resource: ResourceType) -> None:
        key = (resource.type(), resource.name)
        if key in self.__state:
            raise ResourceRedefinedError(resource)
        self.__state[key] = resource

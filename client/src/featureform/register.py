# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
import marshal
from distutils.command.config import config
from typing_extensions import Self
from .resources import ResourceState, Provider, RedisConfig, LocalConfig, PostgresConfig, SnowflakeConfig, User, \
    Location, Source, \
    PrimaryData, SQLTable, SQLTransformation, DFTransformation, Entity, Feature, Label, ResourceColumnMapping, TrainingSet

from typing import Tuple, Callable, TypedDict, List, Union
from typeguard import typechecked, check_type
import grpc
import os
from .proto import metadata_pb2
from .proto import metadata_pb2_grpc as ff_grpc
from .sqlite_metadata import SQLiteMetadata
import time
import pandas as pd

NameVariant = Tuple[str, str]


class EntityRegistrar:

    def __init__(self, registrar, entity):
        self.__registrar = registrar
        self.__entity = entity

    def name(self) -> str:
        return self.__entity.name


class UserRegistrar:

    def __init__(self, registrar, user):
        self.__registrar = registrar
        self.__user = user

    def name(self) -> str:
        return self.__user.name

    def make_default_owner(self):
        self.__registrar.set_default_owner(self.name())


class OfflineProvider:

    def __init__(self, registrar, provider):
        self.__registrar = registrar
        self.__provider = provider

    def name(self) -> str:
        return self.__provider.name


class OfflineSQLProvider(OfflineProvider):

    def __init__(self, registrar, provider):
        super().__init__(registrar, provider)
        self.__registrar = registrar
        self.__provider = provider

    def registrar(self):
        return self.__registrar
    
    def provider(self):
        return self.__provider

    def register_table(self,
                       name: str,
                       variant: str,
                       table: str,
                       owner: Union[str, UserRegistrar] = "",
                       description: str = ""):
        return self.__registrar.register_primary_data(name=name,
                                                      variant=variant,
                                                      location=SQLTable(table),
                                                      owner=owner,
                                                      provider=self.name(),
                                                      description=description)

    def sql_transformation(self,
                           variant: str,
                           owner: Union[str, UserRegistrar] = "",
                           name: str = "",
                           schedule: str = "",
                           description: str = ""):
        return self.__registrar.sql_transformation(name=name,
                                                   variant=variant,
                                                   owner=owner,
                                                   schedule=schedule,
                                                   provider=self.name(),
                                                   description=description)


class OnlineProvider:
    def __init__(self, registrar, provider):
        self.__registrar = registrar
        self.__provider = provider

    def name(self) -> str:
        return self.__provider.name


# RIDDHI
class LocalProvider:
    def __init__(self, registrar, provider):
        self.__registrar = registrar
        self.__provider = provider
        self.sqldb = SQLiteMetadata()
        self.insert_provider()

    def name(self) -> str:
        return self.__provider.name

    def register_file(self, name, variant, description, path, owner=""):
        if owner == "":
            owner = self.__registrar.must_get_default_owner()
        # Store the file as a source
        time_created = str(time.time())
        self.sqldb.insert("sources", "Source", variant, name)
        self.sqldb.insert("source_variant", time_created, description, name,
                          "Source", owner, self.name(), variant, "ready", False, "", path)
        # Where the definition = path

        return LocalSource(self.__registrar, name, owner, variant, self.name(), description)

    def insert_provider(self):
        # Store a new provider row
        self.sqldb.insert("providers",
                          self.__provider.name,
                          "Provider",
                          self.__provider.description,
                          self.__provider.config.type(),
                          self.__provider.config.software(),
                          self.__provider.team,
                          "sources",
                          "status",
                          str(self.__provider.config.serialize(), 'utf-8')
                          )

    def df_transformation(self,
                          variant: str,
                          owner: Union[str, UserRegistrar] = "",
                          name: str = "",
                          description: str = "",
                          inputs: list=[]):
        return self.__registrar.df_transformation(name=name,
                                                  variant=variant,
                                                  owner=owner,
                                                  provider=self.name(),
                                                  description=description,
                                                  inputs=inputs)


class SourceRegistrar:

    def __init__(self, registrar, source):
        self.__registrar = registrar
        self.__source = source

    def id(self) -> NameVariant:
        return self.__source.name, self.__source.variant

    def registrar(self):
        return self.__registrar


class ColumnMapping(TypedDict):
    name: str
    variant: str
    column: str
    resource_type: str


class LocalSource:
    def __init__(self,
                 registrar,
                 name: str,
                 owner: str,
                 variant: str,
                 provider: str,
                 description: str = ""):
        self.registrar = registrar
        self.name = name
        self.variant = variant
        self.owner = owner
        self.provider = provider
        self.description = description

    def __call__(self, fn: Callable[[], str]):
        if self.description == "":
            self.description = fn.__doc__
        if self.name == "":
            self.name = fn.__name__
        self.__set_query(fn())
        fn.register_resources = self.register_resources
        return fn

    def register_resources(
            self,
            entity: Union[str, EntityRegistrar],
            entity_column: str,
            owner: Union[str, UserRegistrar] = "",
            inference_store: Union[str, OnlineProvider] = "",
            features: List[ColumnMapping] = None,
            labels: List[ColumnMapping] = None,
            timestamp_column: str = ""
    ):
        return self.registrar.register_column_resources(
            source=(self.name, self.variant),
            entity=entity,
            entity_column=entity_column,
            owner=owner,
            inference_store=inference_store,
            features=features,
            labels=labels,
            timestamp_column=timestamp_column,
            description=self.description,
        )


class SQLTransformationDecorator:

    def __init__(self,
                 registrar,
                 variant: str,
                 owner: str,
                 provider: str,
                 name: str = "",
                 schedule: str = "",
                 description: str = ""):
        self.registrar = registrar,
        self.name = name
        self.variant = variant
        self.owner = owner
        self.schedule = schedule
        self.provider = provider
        self.description = description

    def __call__(self, fn: Callable[[], str]):
        if self.description == "":
            self.description = fn.__doc__
        if self.name == "":
            self.name = fn.__name__
        self.__set_query(fn())
        fn.register_resources = self.register_resources
        return fn

    @typechecked
    def __set_query(self, query: str):
        if query == "":
            raise ValueError("Query cannot be an empty string")
        self.query = query

    def to_source(self) -> Source:
        return Source(
            name=self.name,
            variant=self.variant,
            definition=SQLTransformation(self.query),
            owner=self.owner,
            schedule=self.schedule,
            provider=self.provider,
            description=self.description,
        )

    def register_resources(
            self,
            entity: Union[str, EntityRegistrar],
            entity_column: str,
            owner: Union[str, UserRegistrar] = "",
            inference_store: Union[str, OnlineProvider] = "",
            features: List[ColumnMapping] = None,
            labels: List[ColumnMapping] = None,
            timestamp_column: str = "",
            description: str = "",
            schedule: str = "",
    ):
        return self.registrar[0].register_column_resources(
            source=(self.name, self.variant),
            entity=entity,
            entity_column=entity_column,
            owner=owner,
            inference_store=inference_store,
            features=features,
            labels=labels,
            timestamp_column=timestamp_column,
            description=description,
            schedule=schedule,
        )


class DFTransformationDecorator:

    def __init__(self,
                 registrar,
                 variant: str,
                 owner: str,
                 provider: str,
                 name: str = "",
                 description: str = "",
                 inputs: list = []):
        self.registrar = registrar,
        self.name = name
        self.variant = variant
        self.owner = owner
        self.provider = provider
        self.description = description
        self.inputs = inputs

    def __call__(self, fn: Callable[[pd.DataFrame], pd.DataFrame]):
        if self.description == "":
            self.description = fn.__doc__
        if self.name == "":
            self.name = fn.__name__
        self.query = marshal.dumps(fn.__code__)
        fn.register_resources = self.register_resources
        return fn

    def to_source(self) -> Source:
        return Source(
            name=self.name,
            variant=self.variant,
            definition=DFTransformation(self.query, self.inputs),
            owner=self.owner,
            provider=self.provider,
            description=self.description,
        )

    def test_func(self):
        pass

    def register_resources(
            self,
            entity: Union[str, EntityRegistrar],
            entity_column: str,
            owner: Union[str, UserRegistrar] = "",
            inference_store: Union[str, OnlineProvider] = "",
            features: List[ColumnMapping] = None,
            labels: List[ColumnMapping] = None,
            timestamp_column: str = "",
            description: str = "",
    ):
        return self.registrar[0].register_column_resources(
            source=(self.name, self.variant),
            entity=entity,
            entity_column=entity_column,
            owner=owner,
            inference_store=inference_store,
            features=features,
            labels=labels,
            timestamp_column=timestamp_column,
            description=description,
        )


class ColumnSourceRegistrar(SourceRegistrar):

    def register_resources(
            self,
            entity: Union[str, EntityRegistrar],
            entity_column: str,
            owner: Union[str, UserRegistrar] = "",
            inference_store: Union[str, OnlineProvider] = "",
            features: List[ColumnMapping] = None,
            labels: List[ColumnMapping] = None,
            timestamp_column: str = "",
            description: str = "",
            schedule: str = "",
    ):
        return self.registrar().register_column_resources(
            source=self,
            entity=entity,
            entity_column=entity_column,
            owner=owner,
            inference_store=inference_store,
            features=features,
            labels=labels,
            timestamp_column=timestamp_column,
            description=description,
            schedule=schedule,
        )


class ResourceRegistrar():

    def __init__(self, registrar, features, labels):
        self.__registrar = registrar
        self.__features = features
        self.__labels = labels

    def create_training_set(self,
                            name: str,
                            variant: str,
                            label: NameVariant = None,
                            schedule: str = "",
                            features: List[NameVariant] = None,
                            owner: Union[str, UserRegistrar] = "",
                            description: str = ""):
        if len(self.__labels) == 0:
            raise ValueError("A label must be included in a training set")
        if len(self.__features) == 0:
            raise ValueError("A feature must be included in a training set")
        if len(self.__labels) > 1 and label == None:
            raise ValueError(
                "Only one label may be specified in a TrainingSet.")
        if features is not None:
            featureSet = set([(feature["name"], feature["variant"])
                              for feature in self.__features])
            for feature in features:
                if feature not in featureSet:
                    raise ValueError(f"Feature {feature} not found.")
        else:
            features = [(feature["name"], feature["variant"])
                        for feature in self.__features]
        if label is None:
            label = (self.__labels[0]["name"], self.__labels[0]["variant"])
        else:
            labelSet = set([
                (label["name"], label["variant"]) for label in self.__labels
            ])
            if label not in labelSet:
                raise ValueError(f"Label {label} not found.")
        return self.__registrar.register_training_set(
            name=name,
            variant=variant,
            label=label,
            features=features,
            owner=owner,
            schedule=schedule,
            description=description,
        )


class Registrar:

    def __init__(self):
        self.__state = ResourceState()
        self.__resources = []
        self.__default_owner = ""

    def register_user(self, name: str) -> UserRegistrar:
        user = User(name)
        self.__resources.append(user)
        return UserRegistrar(self, user)

    def set_default_owner(self, user: str):
        self.__default_owner = user

    def default_owner(self) -> str:
        return self.__default_owner

    def must_get_default_owner(self) -> str:
        owner = self.default_owner()
        if owner == "":
            raise ValueError(
                "Owner must be set or a default owner must be specified.")
        return owner

    def register_redis(self,
                       name: str,
                       description: str = "",
                       team: str = "",
                       host: str = "0.0.0.0",
                       port: int = 6379,
                       password: str = "",
                       db: int = 0):
        config = RedisConfig(host=host, port=port, password=password, db=db)
        provider = Provider(name=name,
                            function="ONLINE",
                            description=description,
                            team=team,
                            config=config)
        self.__resources.append(provider)
        return OnlineProvider(self, provider)

    def register_snowflake(
            self,
            name: str,
            username: str,
            password: str,
            account: str,
            organization: str,
            database: str,
            schema: str = "PUBLIC",
            description: str = "",
            team: str = "",
    ):
        config = SnowflakeConfig(account=account,
                                 database=database,
                                 organization=organization,
                                 username=username,
                                 password=password,
                                 schema=schema)
        provider = Provider(name=name,
                            function="OFFLINE",
                            description=description,
                            team=team,
                            config=config)
        self.__resources.append(provider)
        return OfflineSQLProvider(self, provider)
    
    def register_postgres(self,
                          name: str,
                          description: str = "",
                          team: str = "",
                          host: str = "0.0.0.0",
                          port: int = 5432,
                          user: str = "postgres",
                          password: str = "password",
                          database: str = "postgres"):
        config = PostgresConfig(host=host,
                                port=port,
                                database=database,
                                user=user,
                                password=password)
        provider = Provider(name=name,
                            function="OFFLINE",
                            description=description,
                            team=team,
                            config=config)
        self.__resources.append(provider)
        return OfflineSQLProvider(self, provider)

    def register_redshift(self,
                          name: str,
                          description: str = "",
                          team: str = "",
                          host: str = "",
                          port: int = 5432,
                          user: str = "redshift",
                          password: str = "password",
                          database: str = "dev"):
        config = RedshiftConfig(host=host,
                                port=port,
                                database=database,
                                user=user,
                                password=password)
        provider = Provider(name=name,
                            function="OFFLINE",
                            description=description,
                            team=team,
                            config=config)
        self.__resources.append(provider)
        return OfflineSQLProvider(self, provider)

    def register_local(self):
        config = LocalConfig()
        provider = Provider(name="local mode",
                            function="ONLINE",
                            description="This is local mode",
                            team="team",
                            config=config)
        self.__resources.append(provider)
        return LocalProvider(self, provider)

    def register_primary_data(self,
                              name: str,
                              variant: str,
                              location: Location,
                              provider: Union[str, OfflineProvider],
                              owner: Union[str, UserRegistrar] = "",
                              description: str = ""):
        if not isinstance(owner, str):
            owner = owner.name()
        if owner == "":
            owner = self.must_get_default_owner()
        if not isinstance(provider, str):
            provider = provider.name()
        source = Source(name=name,
                        variant=variant,
                        definition=PrimaryData(location=location),
                        owner=owner,
                        provider=provider,
                        description=description)
        self.__resources.append(source)
        return ColumnSourceRegistrar(self, source)

    def register_sql_transformation(self,
                                    name: str,
                                    variant: str,
                                    query: str,
                                    provider: Union[str, OfflineProvider],
                                    owner: Union[str, UserRegistrar] = "",
                                    description: str = "",
                                    schedule: str = ""):
        if not isinstance(owner, str):
            owner = owner.name()
        if owner == "":
            owner = self.must_get_default_owner()
        if not isinstance(provider, str):
            provider = provider.name()
        source = Source(
            name=name,
            variant=variant,
            definition=SQLTransformation(query),
            owner=owner,
            schedule=schedule,
            provider=provider,
            description=description,
        )
        self.__resources.append(source)
        return ColumnSourceRegistrar(self, source)

    def sql_transformation(self,
                           variant: str,
                           provider: Union[str, OfflineProvider],
                           name: str = "",
                           schedule: str = "",
                           owner: Union[str, UserRegistrar] = "",
                           description: str = ""):
        if not isinstance(owner, str):
            owner = owner.name()
        if owner == "":
            owner = self.must_get_default_owner()
        if not isinstance(provider, str):
            provider = provider.name()
        decorator = SQLTransformationDecorator(
            registrar=self,
            name=name,
            variant=variant,
            provider=provider,
            schedule=schedule,
            owner=owner,
            description=description,
        )
        self.__resources.append(decorator)
        return decorator

    def df_transformation(self,
                          variant: str,
                          provider: Union[str, OfflineProvider],
                          name: str = "",
                          owner: Union[str, UserRegistrar] = "",
                          description: str = "",
                          inputs: list = []):
        if not isinstance(owner, str):
            owner = owner.name()
        if owner == "":
            owner = self.must_get_default_owner()
        if not isinstance(provider, str):
            provider = provider.name()
        decorator = DFTransformationDecorator(
            registrar=self,
            name=name,
            variant=variant,
            provider=provider,
            owner=owner,
            description=description,
            inputs=inputs,
        )
        self.__resources.append(decorator)
        return decorator

    def state(self):
        for resource in self.__resources:
            if isinstance(resource, SQLTransformationDecorator) or isinstance(resource, DFTransformationDecorator):
                resource = resource.to_source()
            self.__state.add(resource)
        self.__resources = []
        return self.__state

    def register_entity(self, name: str, description: str = ""):
        entity = Entity(name=name, description=description)
        self.__resources.append(entity)
        return EntityRegistrar(self, entity)

    def register_column_resources(
            self,
            source: Union[NameVariant, SourceRegistrar, SQLTransformationDecorator],
            entity: Union[str, EntityRegistrar],
            entity_column: str,
            owner: Union[str, UserRegistrar] = "",
            inference_store: Union[str, OnlineProvider] = "",
            features: List[ColumnMapping] = None,
            labels: List[ColumnMapping] = None,
            timestamp_column: str = "",
            description: str = "",
            schedule: str = "",
    ):
        if features is None:
            features = []
        if labels is None:
            labels = []
        if len(features) == 0 and len(labels) == 0:
            raise ValueError("No features or labels set")
        if not isinstance(source, tuple):
            source = source.id()
        if not isinstance(entity, str):
            entity = entity.name()
        if not isinstance(inference_store, str):
            inference_store = inference_store.name()
        if len(features) > 0 and inference_store == "":
            raise ValueError(
                "Inference store must be set when defining features")
        if not isinstance(owner, str):
            owner = owner.name()
        if owner == "":
            owner = self.must_get_default_owner()
        feature_resources = []
        label_resources = []
        for feature in features:
            resource = Feature(
                name=feature["name"],
                variant=feature["variant"],
                source=source,
                value_type=feature["type"],
                entity=entity,
                owner=owner,
                provider=inference_store,
                description=description,
                schedule=schedule,
                location=ResourceColumnMapping(
                    entity=entity_column,
                    value=feature["column"],
                    timestamp=timestamp_column,
                ),
            )
            self.__resources.append(resource)
            feature_resources.append(resource)

        for label in labels:
            resource = Label(
                name=label["name"],
                variant=label["variant"],
                source=source,
                value_type=label["type"],
                entity=entity,
                owner=owner,
                provider=inference_store,
                description=description,
                location=ResourceColumnMapping(
                    entity=entity_column,
                    value=label["column"],
                    timestamp=timestamp_column,
                ),
            )
            self.__resources.append(resource)
            label_resources.append(resource)
        return ResourceRegistrar(self, features, labels)

    def register_training_set(self,
                              name: str,
                              variant: str,
                              label: NameVariant,
                              features: List[NameVariant],
                              owner: Union[str, UserRegistrar] = "",
                              description: str = "",
                              schedule: str = ""):
        if not isinstance(owner, str):
            owner = owner.name()
        if owner == "":
            owner = self.must_get_default_owner()
        resource = TrainingSet(
            name=name,
            variant=variant,
            description=description,
            owner=owner,
            schedule=schedule,
            label=label,
            features=features,
        )
        self.__resources.append(resource)


class Client(Registrar):
    def __init__(self, host, tls_verify=False, cert_path=None):
        super().__init__()
        env_cert_path = os.getenv('FEATUREFORM_CERT')
        if tls_verify:
            credentials = grpc.ssl_channel_credentials()
            channel = grpc.secure_channel(host, credentials)
        elif cert_path is not None or env_cert_path is not None:
            if env_cert_path is not None and cert_path is None:
                cert_path = env_cert_path
            with open(cert_path, 'rb') as f:
                credentials = grpc.ssl_channel_credentials(f.read())
            channel = grpc.secure_channel(host, credentials)
        else:
            channel = grpc.insecure_channel(host, options=(('grpc.enable_http_proxy', 0),))
        self._stub = ff_grpc.ApiStub(channel)

    def apply(self):
        self.state().create_all(self._stub)

    def get_redis(self, name):
        searchName = metadata_pb2.Name(name=name)
        try:
            for provider in self._stub.GetProviders(iter([searchName])):
                return OnlineProvider(global_registrar, provider)
        except grpc._channel._MultiThreadedRendezvous:
            raise ValueError("Provider not found.")
    
    def get_postgres(self, name):
        searchName = metadata_pb2.Name(name=name)
        try:
            for provider in self._stub.GetProviders(iter([searchName])):
                return OfflineSQLProvider(global_registrar, provider)
        except grpc._channel._MultiThreadedRendezvous:
            raise ValueError("Provider not found.")




global_registrar = Registrar()
state = global_registrar.state
register_user = global_registrar.register_user
register_redis = global_registrar.register_redis
register_snowflake = global_registrar.register_snowflake
register_postgres = global_registrar.register_postgres
register_redshift = global_registrar.register_redshift
register_local = global_registrar.register_local
register_entity = global_registrar.register_entity
register_column_resources = global_registrar.register_column_resources
register_training_set = global_registrar.register_training_set
sql_transformation = global_registrar.sql_transformation
register_sql_transformation = global_registrar.register_sql_transformation

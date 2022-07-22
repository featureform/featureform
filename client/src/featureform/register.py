# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

import marshal
from distutils.command.config import config
from typing_extensions import Self
from numpy import byte
from .resources import ResourceState, Provider, RedisConfig, FirestoreConfig, CassandraConfig, DynamodbConfig, \
    PostgresConfig, SnowflakeConfig, LocalConfig, RedshiftConfig, User, Location, Source, PrimaryData, SQLTable, \
    SQLTransformation, DFTransformation, Entity, Feature, Label, ResourceColumnMapping, TrainingSet, ProviderReference, \
    EntityReference, SourceReference
from typing import Tuple, Callable, List, Union
from typeguard import typechecked, check_type
import grpc
import os
from featureform.proto import metadata_pb2
from featureform.proto import metadata_pb2_grpc as ff_grpc
from .sqlite_metadata import SQLiteMetadata
import time
import pandas as pd
from .get import *
from.get_local import *
from .list import *

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

    def register_table(self,
                       name: str,
                       variant: str,
                       table: str,
                       owner: Union[str, UserRegistrar] = "",
                       description: str = ""):
        """Register a SQL table as a primary data source.

        Args:
            name (str): Name of table to be registered
            variant (str): Name of variant to be registered
            table (str): Name of SQL table
            owner (Union[str, UserRegistrar]): Owner
            description (str): Description of table to be registered

        Returns:
            source (ColumnSourceRegistrar): source
        """
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

    def name(self) -> str:
        return self.__provider.name

    def register_file(self, name, variant, description, path, owner=""):
        if owner == "":
            owner = self.__registrar.must_get_default_owner()
        # Store the file as a source
        time_created = str(time.time())
        self.sqldb.insert("sources", "Source", variant, name)
        self.sqldb.insert("source_variant", time_created, description, name,
                          "Source", owner, self.name(), variant, "ready", 0, "", path)
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
                          inputs: list = []):
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


class ColumnMapping(dict):
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

    def get_source(self, name, variant, local=False):
        get = SourceReference(name=name, variant=variant, obj=None)
        self.__resources.append(get)
        if local:
            return LocalSource(self,
                                name=name,
                                owner="",
                                variant=variant,
                                provider="",
                                description="")
        else:
            fakeDefinition = PrimaryData(location=SQLTable(name=""))
            fakeSource = Source(name=name,
                            variant=variant,
                            definition=fakeDefinition,
                            owner="",
                            provider="",
                            description="")
            return ColumnSourceRegistrar(self, fakeSource)

    def get_local(self, name):
        get = ProviderReference(name=name, provider_type="local", obj=None)
        self.__resources.append(get)
        fakeConfig = LocalConfig()
        fakeProvider = Provider(name=name, function="LOCAL_ONLINE", description="", team="", config=fakeConfig)
        return LocalProvider(self, fakeProvider)

    def get_redis(self, name):
        get = ProviderReference(name=name, provider_type="redis", obj=None)
        self.__resources.append(get)
        fakeConfig = RedisConfig(host="", port=123, password="", db=123)
        fakeProvider = Provider(name=name, function="ONLINE", description="", team="", config=fakeConfig)
        return OnlineProvider(self, fakeProvider)

    def get_postgres(self, name):
        get = ProviderReference(name=name, provider_type="postgres", obj=None)
        self.__resources.append(get)
        fakeConfig = PostgresConfig(host="", port="", database="", user="", password="")
        fakeProvider = Provider(name=name, function="OFFLINE", description="", team="", config=fakeConfig)
        return OfflineSQLProvider(self, fakeProvider)

    def get_snowflake(self, name):
        get = ProviderReference(name=name, provider_type="snowflake", obj=None)
        self.__resources.append(get)
        fakeConfig = SnowflakeConfig(account="", database="", organization="", username="", password="", schema="")
        fakeProvider = Provider(name=name, function="OFFLINE", description="", team="", config=fakeConfig)
        return OfflineSQLProvider(self, fakeProvider)

    def get_entity(self, name):
        get = EntityReference(name=name, obj=None)
        self.__resources.append(get)
        fakeEntity = Entity(name=name, description="")
        return EntityRegistrar(self, fakeEntity)

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
        
    def register_firestore(self,
                       name: str,
                       description: str = "",
                       team: str = "",
                       collection: str = "",
                       project_id: str = "",
                       credentials_path: str = ""
                       ):
        config = FirestoreConfig(collection=collection, project_id=project_id, credentials_path=credentials_path)
        provider = Provider(name=name,
                            function="ONLINE",
                            description=description,
                            team=team,
                            config=config)
        self.__resources.append(provider)
        return OnlineProvider(self, provider)

    def register_cassandra(self,
                       name: str,
                       description: str = "",
                       team: str = "",
                       host: str = "0.0.0.0",
                       port: int = 9042,
                       username: str = "cassandra",
                       password: str = "cassandra",
                       keyspace: str = "",
                       consistency: str = "THREE",
                       replication: int = 3):
        config = CassandraConfig(host=host, port=port, username=username, password=password, keyspace=keyspace, consistency=consistency, replication=replication)
        provider = Provider(name=name,
                            function="ONLINE",
                            description=description,
                            team=team,
                            config=config)
        self.__resources.append(provider)
        return OnlineProvider(self, provider)
        
    def register_dynamodb(self,
                          name: str,
                          description: str = "",
                          team: str = "",
                          access_key: str = None,
                          secret_key: str = None,
                          region: str = None):
        """Register a DynamoDB provider.

        **Examples**:
        ```   
        dynamodb = ff.register_dynamodb(
            name="dynamodb-quickstart",
            host="quickstart-dynamodb",  # The internal dns name for dynamodb
            description="A Dynamodb deployment we created for the Featureform quickstart",
            access_key="$ACCESS_KEY",
            secret_key="$SECRET_KEY",
            region="us-east-1"
        )
        ```
        Args:
            name (str): Name of DynamoDB provider to be registered
            description (str): Description of DynamoDB provider to be registered
            team (str): Name of team
            access_key (str): Access key
            secret_key (str): Secret key
            region (str): Region

        Returns:
            dynamodb (OnlineProvider): Provider
        """
        config = DynamodbConfig(access_key=access_key, secret_key=secret_key, region=region)
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
        """Register a Snowflake provider.

        **Examples**:
        ```   
        snowflake = ff.register_snowflake(
            name="snowflake-quickstart",
            username="snowflake",
            password="password",
            account="account",
            database="snowflake",
            schema="PUBLIC",
            description="A Dynamodb deployment we created for the Featureform quickstart"
        )
        ```
        Args:
            name (str): Name of Snowflake provider to be registered
            username (str): Username
            password (str): Password
            account (str): Account
            organization (str): Organization
            database (str): Database
            schema (str): Schema
            description (str): Description of Snowflake provider to be registered
            team (str): Name of team

        Returns:
            snowflake (OfflineSQLProvider): Provider
        """
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
        """Register a Postgres provider.

        **Examples**:
        ```   
        postgres = ff.register_postgres(
            name="postgres-quickstart",
            description="A Postgres deployment we created for the Featureform quickstart",
            host="quickstart-postgres",  # The internal dns name for postgres
            port="5432",
            user="postgres",
            password="password",
            database="postgres"
        )
        ```
        Args:
            name (str): Name of Postgres provider to be registered
            username (str): Username
            description (str): Description of Postgres provider to be registered
            team (str): Name of team
            host (str): Internal DNS name of Postgres
            port (str): Port
            user (str): User
            password (str): Password
            database (str): Database
            
        Returns:
            postgres (OfflineSQLProvider): Provider
        """
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
        """Register a Redshift provider.

        **Examples**:
        ```   
        redshift = ff.register_redshift(
            name="redshift-quickstart",
            description="A Redshift deployment we created for the Featureform quickstart",
            host="quickstart-redshift",  # The internal dns name for postgres
            port="5432",
            user="redshift",
            password="password",
            database="dev"
        )
        ```
        Args:
            name (str): Name of Redshift provider to be registered
            description (str): Description of Redshift provider to be registered
            team (str): Name of team
            host (str): Internal DNS name of Redshift
            port (str): Port
            user (str): User
            password (str): Password
            database (str): Database
            
        Returns:
            redshift (OfflineSQLProvider): Provider
        """
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
        """Register a Local provider.

        **Examples**:
        ```   
            local = register_local()
        ```
        Args:
            
        Returns:
            local (LocalProvider): Provider
        """
        config = LocalConfig()
        provider = Provider(name="local-mode",
                            function="LOCAL_ONLINE",
                            description="This is local mode",
                            team="team",
                            config=config)
        self.__resources.append(provider)
        local_provider = LocalProvider(self, provider)
        local_provider.insert_provider()
        return local_provider

    def register_primary_data(self,
                              name: str,
                              variant: str,
                              location: Location,
                              provider: Union[str, OfflineProvider],
                              owner: Union[str, UserRegistrar] = "",
                              description: str = ""):
        """Register a primary data source.

        Args:
            name (str): Name of source
            variant (str): Name of variant
            location (Location): Location of primary data
            provider (Union[str, OfflineProvider]): Provider
            owner (Union[str, UserRegistrar]): Owner
            description (str): Description of primary data to be registered

        Returns:
            source (ColumnSourceRegistrar): Source
        """
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
        """Register a SQL transformation source.

        Args:
            name (str): Name of source
            variant (str): Name of variant
            query (str): SQL query
            provider (Union[str, OfflineProvider]): Provider
            owner (Union[str, UserRegistrar]): Owner
            description (str): Description of primary data to be registered
            schedule (str): Kubernetes CronJob schedule string ("* * * * *")

        Returns:
            source (ColumnSourceRegistrar): Source
        """
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
        """SQL transformation decorator.

        Args:
            variant (str): Name of variant
            provider (Union[str, OfflineProvider]): Provider
            name (str): Name of source
            schedule (str): Kubernetes CronJob schedule string ("* * * * *")
            owner (Union[str, UserRegistrar]): Owner
            description (str): Description of SQL transformation

        Returns:
            decorator (SQLTransformationDecorator): decorator
        """
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
        """Dataframe transformation decorator.

        Args:
            variant (str): Name of variant
            provider (Union[str, OfflineProvider]): Provider
            name (str): Name of source
            owner (Union[str, UserRegistrar]): Owner
            description (str): Description of SQL transformation
            inputs (list): Inputs to transformation

        Returns:
            decorator (DFTransformationDecorator): decorator
        """
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
        """Register an entity.

        Args:
            name (str): Name of entity to be registered
            description (str): Description of entity to be registered

        Returns:
            entity (EntityRegistrar): Entity
        """
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
        """Create features and labels from a source. Used in the register_resources function.

        Args:
            source (Union[NameVariant, SourceRegistrar, SQLTransformationDecorator]): Source of features, labels, entity
            entity (Union[str, EntityRegistrar]): Entity
            entity_column (str): Column of entity in source
            owner (Union[str, UserRegistrar]): Owner
            inference_store (Union[str, OnlineProvider]): Online provider
            features (List[ColumnMapping]): List of ColumnMapping objects (dictionaries containing the keys: name, variant, column, resource_type)
            labels (List[ColumnMapping]): List of ColumnMapping objects (dictionaries containing the keys: name, variant, column, resource_type)
            description (str): Description 
            schedule (str): Kubernetes CronJob schedule string ("* * * * *")

        Returns:
            resource (ResourceRegistrar): resource
        """
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
        """Register a training set.

        Args:
            name (str): Name of training set to be registered
            variant (str): Name of variant to be registered
            label (NameVariant): Label of training set
            features (List[NameVariant]): Features of training set
            owner (Union[str, UserRegistrar]): Owner
            description (str): Description of training set to be registered
            schedule (str): Kubernetes CronJob schedule string ("* * * * *")

        Returns:
            resource (ResourceRegistrar): resource
        """
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
    def __init__(self, host=None, local=False, insecure=True, cert_path=None):
        super().__init__()
        self._stub = None
        self.local = local
        if self.local:
            if host != None:
                raise ValueError("Cannot be local and have a host")

        elif host == None:
            host = os.getenv('FEATUREFORM_HOST')
            if host == None:
                raise ValueError(
                    "Host value must be set or in env as FEATUREFORM_HOST")
        else:
            channel = self.tls_check(host, cert_path, insecure)
            self._stub = ff_grpc.ApiStub(channel)

    def tls_check(self, host, cert, insecure):
        if insecure:
            channel = grpc.insecure_channel(
                host, options=(('grpc.enable_http_proxy', 0),))
        elif cert != None or os.getenv('FEATUREFORM_CERT') != None:
            if os.getenv('FEATUREFORM_CERT') != None and cert == None:
                cert = os.getenv('FEATUREFORM_CERT')
            with open(cert, 'rb') as f:
                credentials = grpc.ssl_channel_credentials(f.read())
            channel = grpc.secure_channel(host, credentials)
        else:
            credentials = grpc.ssl_channel_credentials()
            channel = grpc.secure_channel(host, credentials)
        return channel

    def apply(self):
        """Apply all definitions, creating and retrieving all specified resources.
        """
        
        if self.local:
            state().create_all_local()
        else:
            state().create_all(self._stub)

    def get_user(self, name, local=False):
        """Get a user. Prints out name of user, and all resources associated with the user.

        **Examples:**

        Input
        ```
        // get_user.py
        featureformer = rc.get_user("featureformer")
        print(featureformer)
        ```
        Output
        ```
        // get_user prints a formatted version of user information
        USER NAME:                     featureformer
        -----------------------------------------------

        NAME                           VARIANT                        TYPE
        avg_transactions               quickstart                     feature
        fraudulent                     quickstart                     label
        fraud_training                 quickstart                     training set
        transactions                   kaggle                         source
        average_user_transaction       quickstart                     source
        -----------------------------------------------

        // get_user returns the User object
        name: "featureformer"
        features {
        name: "avg_transactions"
        variant: "quickstart"
        }
        labels {
        name: "fraudulent"
        variant: "quickstart"
        }
        trainingsets {
        name: "fraud_training"
        variant: "quickstart"
        }
        sources {
        name: "transactions"
        variant: "kaggle"
        }
        sources {
        name: "average_user_transaction"
        variant: "quickstart"
        }
        ```


        Args:
            name (str): Name of user to be retrieved

        Returns:
            user (User): User
        """
        if local:
            return get_user_info_local(name)
        return get_user_info(self._stub, name)
    
    def get_entity(self, name, local=False):
        """Get an entity.

        Args:
            name (str): Name of entity to be retrieved

        Returns:
            user (User): User that is retrieved.
        """
        if local:
            return get_entity_info_local(name)
        return get_entity_info(self._stub, name)

    def get_model(self, name):
        return get_resource_info(self._stub, "model", name)

    def get_provider(self, name):
        return get_provider_info(self._stub, name)

    def get_feature(self, name, variant=None, local=False):
        if local:
            if not variant:
                return get_resource_info_local("feature", name)
        if not variant:
            return get_resource_info(self._stub, "feature", name)
        return get_feature_variant_info(self._stub, name, variant)

    def get_label(self, name, variant=None, local=False):
        if local:
            if not variant:
                return get_resource_info_local("label", name)
        if not variant:
            return get_resource_info(self._stub, "label", name)
        return get_label_variant_info(self._stub, name, variant)

    def get_training_set(self, name, variant=None, local=False):
        if local:
            if not variant:
                return get_resource_info_local("training-set", name)
        if not variant:
            return get_resource_info(self._stub, "training-set", name)
        return get_training_set_variant_info(self._stub, name, variant)

    def get_source(self, name, variant=None, local=False):
        if local:
            if not variant:
                return get_resource_info_local("source", name)
        if not variant:
            return get_resource_info(self._stub, "source", name)
        return get_source_variant_info(self._stub, name, variant)

    def list_features(self):
        return list_name_variant_status(self._stub, "feature")

    def list_labels(self):
        return list_name_variant_status(self._stub, "label")

    def list_users(self):
        return list_name_status(self._stub, "user")

    def list_entities(self):
        return list_name_status(self._stub, "entity")

    def list_sources(self):
        return list_name_variant_status_desc(self._stub, "source")

    def list_training_sets(self):
        return list_name_variant_status_desc(self._stub, "training-set")

    def list_models(self):
        return list_name_status_desc(self._stub, "model")

    def list_providers(self):
        return list_name_status_desc(self._stub, "provider")

global_registrar = Registrar()
state = global_registrar.state
register_user = global_registrar.register_user
register_redis = global_registrar.register_redis
register_firestore = global_registrar.register_firestore
register_cassandra = global_registrar.register_cassandra
register_dynamodb = global_registrar.register_dynamodb
register_snowflake = global_registrar.register_snowflake
register_postgres = global_registrar.register_postgres
register_redshift = global_registrar.register_redshift
register_local = global_registrar.register_local
register_entity = global_registrar.register_entity
register_column_resources = global_registrar.register_column_resources
register_training_set = global_registrar.register_training_set
sql_transformation = global_registrar.sql_transformation
register_sql_transformation = global_registrar.register_sql_transformation
get_entity = global_registrar.get_entity
get_source = global_registrar.get_source
get_local = global_registrar.get_local
get_redis = global_registrar.get_redis
get_postgres = global_registrar.get_postgres
get_snowflake = global_registrar.get_snowflake

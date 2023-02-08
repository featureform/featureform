# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.


from datetime import timedelta
from multiprocessing.sharedctypes import Value
from typeguard import typechecked, check_type
from typing import Tuple, Callable, List, Union

import dill
import pandas as pd

from .get import *
from .list import *
from .get_local import *
from .list_local import *
from .sqlite_metadata import SQLiteMetadata
from .tls import insecure_channel, secure_channel
from .resources import ResourceState, Provider, RedisConfig, FirestoreConfig, CassandraConfig, DynamodbConfig, \
    MongoDBConfig, PostgresConfig, SnowflakeConfig, LocalConfig, RedshiftConfig, BigQueryConfig, SparkConfig, \
    AzureFileStoreConfig, OnlineBlobConfig, K8sConfig, S3StoreConfig, User, Location, Source, PrimaryData, SQLTable, \
    SQLTransformation, DFTransformation, Entity, Feature, Label, ResourceColumnMapping, TrainingSet, ProviderReference, \
    EntityReference, SourceReference, ExecutorCredentials, ResourceRedefinedError, ResourceStatus, Transformation, \
    K8sArgs, AWSCredentials

from .proto import metadata_pb2_grpc as ff_grpc


NameVariant = Tuple[str, str]

s3_config = S3StoreConfig("", "", AWSCredentials("id", "secret"))
NON_INFERENCE_STORES = [s3_config.type()]


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
                       table: str,
                       variant: str = "default",
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
                           owner: Union[str, UserRegistrar] = "",
                           variant: str = "default",
                           name: str = "",
                           schedule: str = "",
                           description: str = ""):
        return self.__registrar.sql_transformation(name=name,
                                                   variant=variant,
                                                   owner=owner,
                                                   schedule=schedule,
                                                   provider=self.name(),
                                                   description=description)


class OfflineSparkProvider(OfflineProvider):
    def __init__(self, registrar, provider):
        super().__init__(registrar, provider)
        self.__registrar = registrar
        self.__provider = provider

    def register_parquet_file(self,
                              name: str,
                              file_path: str,
                              variant: str = "default",
                              owner: Union[str, UserRegistrar] = "",
                              description: str = ""):
        """Register a Spark data source as a primary data source.

        Args:
            name (str): Name of table to be registered
            variant (str): Name of variant to be registered
            file_path (str): The path to s3 file
            owner (Union[str, UserRegistrar]): Owner
            description (str): Description of table to be registered

        Returns:
            source (ColumnSourceRegistrar): source
        """
        return self.__registrar.register_primary_data(name=name,
                                                      variant=variant,
                                                      location=SQLTable(file_path),
                                                      owner=owner,
                                                      provider=self.name(),
                                                      description=description)

    def sql_transformation(self,
                           variant: str,
                           owner: Union[str, UserRegistrar] = "",
                           name: str = "",
                           schedule: str = "",
                           description: str = ""):
        """
        Register a SQL transformation source. The spark.sql_transformation decorator takes the returned string in the
        following function and executes it as a SQL Query.

        The name of the function is the name of the resulting source.

        Sources for the transformation can be specified by adding the Name and Variant in brackets '{{ name.variant }}'.
        The correct source is substituted when the query is run.

        **Examples**:
        ``` py
        @spark.sql_transformation(variant="quickstart")
        def average_user_transaction():
            return "SELECT CustomerID as user_id, avg(TransactionAmount) as avg_transaction_amt from" \
            " {{transactions.v1}} GROUP BY user_id"
        ```

        Args:
            name (str): Name of source
            variant (str): Name of variant
            owner (Union[str, UserRegistrar]): Owner
            description (str): Description of primary data to be registered


        Returns:
            source (ColumnSourceRegistrar): Source
        """
        return self.__registrar.sql_transformation(name=name,
                                                   variant=variant,
                                                   owner=owner,
                                                   schedule=schedule,
                                                   provider=self.name(),
                                                   description=description)

    def df_transformation(self,
                          variant: str = "default",
                          owner: Union[str, UserRegistrar] = "",
                          name: str = "",
                          description: str = "",
                          inputs: list = []):
        """
        Register a Dataframe transformation source. The k8s_azure.df_transformation decorator takes the contents
        of the following function and executes the code it contains at serving time.

        The name of the function is used as the name of the source when being registered.

        The specified inputs are loaded into dataframes that can be accessed using the function parameters.

        **Examples**:
        ``` py
        @k8s_azure.df_transformation(inputs=[("source", "one")])        # Sources are added as inputs
        def average_user_transaction(df):                           # Sources can be manipulated by adding them as params
            return df
        ```

        Args:
            name (str): Name of source
            variant (str): Name of variant
            owner (Union[str, UserRegistrar]): Owner
            description (str): Description of primary data to be registered
            inputs (list[Tuple(str, str)]): A list of Source NameVariant Tuples to input into the transformation

        Returns:
            source (ColumnSourceRegistrar): Source
        """
        return self.__registrar.df_transformation(name=name,
                                                  variant=variant,
                                                  owner=owner,
                                                  provider=self.name(),
                                                  description=description,
                                                  inputs=inputs)


class OfflineK8sProvider(OfflineProvider):
    def __init__(self, registrar, provider):
        super().__init__(registrar, provider)
        self.__registrar = registrar
        self.__provider = provider

    def register_file(self,
                      name: str,
                      path: str,
                      variant: str = "default",
                      owner: Union[str, UserRegistrar] = "",
                      description: str = ""):
        """Register a blob data source path as a primary data source.

        Args:
            name (str): Name of table to be registered
            variant (str): Name of variant to be registered
            path (str): The path to blob store file
            owner (Union[str, UserRegistrar]): Owner
            description (str): Description of table to be registered

        Returns:
            source (ColumnSourceRegistrar): source
        """
        return self.__registrar.register_primary_data(name=name,
                                                      variant=variant,
                                                      location=SQLTable(path),
                                                      owner=owner,
                                                      provider=self.name(),
                                                      description=description)

    def sql_transformation(self,
                           variant: str = "",
                           owner: Union[str, UserRegistrar] = "",
                           name: str = "",
                           schedule: str = "",
                           description: str = "",
                           docker_image: str = ""
                           ):
        """
        Register a SQL transformation source. The k8s.sql_transformation decorator takes the returned string in the
        following function and executes it as a SQL Query.

        The name of the function is the name of the resulting source.

        Sources for the transformation can be specified by adding the Name and Variant in brackets '{{ name.variant }}'.
        The correct source is substituted when the query is run.

        **Examples**:
        ``` py
        @k8s.sql_transformation(variant="quickstart")
        def average_user_transaction():
            return "SELECT CustomerID as user_id, avg(TransactionAmount) as avg_transaction_amt from" \
            " {{transactions.v1}} GROUP BY user_id"
        ```

        Args:
            name (str): Name of source
            variant (str): Name of variant
            owner (Union[str, UserRegistrar]): Owner
            description (str): Description of primary data to be registered
            docker_image (str): A custom Docker image to run the transformation


        Returns:
            source (ColumnSourceRegistrar): Source
        """
        return self.__registrar.sql_transformation(name=name,
                                                   variant=variant,
                                                   owner=owner,
                                                   schedule=schedule,
                                                   provider=self.name(),
                                                   description=description,
                                                   args=K8sArgs(docker_image=docker_image)
                                                   )

    def df_transformation(self,
                          variant: str = "default",
                          owner: Union[str, UserRegistrar] = "",
                          name: str = "",
                          description: str = "",
                          inputs: list = [],
                          docker_image: str = ""
                          ):
        """
        Register a Dataframe transformation source. The k8s_azure.df_transformation decorator takes the contents
        of the following function and executes the code it contains at serving time.

        The name of the function is used as the name of the source when being registered.

        The specified inputs are loaded into dataframes that can be accessed using the function parameters.

        **Examples**:
        ``` py
        @k8s_azure.df_transformation(inputs=[("source", "one")])        # Sources are added as inputs
        def average_user_transaction(df):                           # Sources can be manipulated by adding them as params
            return df
        ```

        Args:
            name (str): Name of source
            variant (str): Name of variant
            owner (Union[str, UserRegistrar]): Owner
            description (str): Description of primary data to be registered
            inputs (list[Tuple(str, str)]): A list of Source NameVariant Tuples to input into the transformation
            docker_image (str): A custom Docker image to run the transformation

        Returns:
            source (ColumnSourceRegistrar): Source
        """
        return self.__registrar.df_transformation(name=name,
                                                  variant=variant,
                                                  owner=owner,
                                                  provider=self.name(),
                                                  description=description,
                                                  inputs=inputs,
                                                  args=K8sArgs(docker_image=docker_image)
                                                  )


class OnlineProvider:
    def __init__(self, registrar, provider):
        self.__registrar = registrar
        self.__provider = provider

    def name(self) -> str:
        return self.__provider.name


class FileStoreProvider:
    def __init__(self, registrar, provider, config, store_type):
        self.__registrar = registrar
        self.__provider = provider
        self.__config = config.config()
        self.__store_type = store_type

    def name(self) -> str:
        return self.__provider.name

    def store_type(self) -> str:
        return self.__store_type

    def config(self):
        return self.__config


class LocalProvider:
    """
    The LocalProvider exposes the registration functions for LocalMode

    **Using the LocalProvider:**
    ``` py
    from featureform import local

    transactions = local.register_file(
        name="transactions",
        variant="quickstart",
        description="A dataset of fraudulent transactions",
        path="transactions.csv"
    )
    ```
    """

    def __init__(self, registrar, provider):
        self.__registrar = registrar
        self.__provider = provider

    def name(self) -> str:
        return self.__provider.name

    def register_file(self, name, description, path, variant="default", owner=""):
        """Register a local file.

        **Examples**:
        ```
        transactions = local.register_file(
            name="transactions",
            variant="quickstart",
            description="A dataset of fraudulent transactions",
            path="transactions.csv"
        )
        ```
        Args:
            name (str): Name for how to reference the file later
            description (str): Description of the file
            path (str): Path to the file
            variant (str): File variant
            owner (str): Owner of the file

        Returns:
            source (LocalSource): source
        """
        if owner == "":
            owner = self.__registrar.must_get_default_owner()
        # Store the file as a source
        self.__registrar.register_primary_data(name, variant, SQLTable(path), self.__provider.name, owner, description)
        return LocalSource(self.__registrar, name, owner, variant, self.name(), path, description)

    def insert_provider(self):
        sqldb = SQLiteMetadata()
        # Store a new provider row
        sqldb.insert("providers",
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
        sqldb.close()

    def df_transformation(self,
                          variant: str = "default",
                          owner: Union[str, UserRegistrar] = "",
                          name: str = "",
                          description: str = "",
                          inputs: list = []):
        """
        Register a Dataframe transformation source. The local.df_transformation decorator takes the contents
        of the following function and executes the code it contains at serving time.

        The name of the function is used as the name of the source when being registered.

        The specified inputs are loaded into dataframes that can be accessed using the function parameters.

        **Examples**:
        ``` py
        @local.df_transformation(inputs=[("source", "one"), ("source", "two")]) # Sources are added as inputs
        def average_user_transaction(df_one, df_two):                           # Sources can be manipulated by adding them as params
            return source_one.groupby("CustomerID")["TransactionAmount"].mean()
        ```

        Args:
            name (str): Name of source
            variant (str): Name of variant
            owner (Union[str, UserRegistrar]): Owner
            description (str): Description of primary data to be registered
            inputs (list[Tuple(str, str)]): A list of Source NameVariant Tuples to input into the transformation

        Returns:
            source (ColumnSourceRegistrar): Source
        """
        return self.__registrar.df_transformation(name=name,
                                                  variant=variant,
                                                  owner=owner,
                                                  provider=self.name(),
                                                  description=description,
                                                  inputs=inputs)

    def sql_transformation(self,
                           variant: str = "default",
                           owner: Union[str, UserRegistrar] = "",
                           name: str = "",
                           description: str = ""):
        """
        Register a SQL transformation source. The local.sql_transformation decorator takes the returned string in the
        following function and executes it as a SQL Query.

        The name of the function is the name of the resulting source.

        Sources for the transformation can be specified by adding the Name and Variant in brackets '{{ name.variant }}'.
        The correct source is substituted when the query is run.

        **Examples**:
        ``` py
        @local.sql_transformation(variant="quickstart")
        def average_user_transaction():
            return "SELECT CustomerID as user_id, avg(TransactionAmount) as avg_transaction_amt from" \
            " {{transactions.v1}} GROUP BY user_id"
        ```

        Args:
            name (str): Name of source
            variant (str): Name of variant
            owner (Union[str, UserRegistrar]): Owner
            description (str): Description of primary data to be registered


        Returns:
            source (ColumnSourceRegistrar): Source
        """
        return self.__registrar.sql_transformation(name=name,
                                                   variant=variant,
                                                   owner=owner,
                                                   provider=self.name(),
                                                   description=description)


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
    """
    LocalSource creates a reference to a source that can be accessed locally.
    """

    def __init__(self,
                 registrar,
                 name: str,
                 owner: str,
                 variant: str,
                 provider: str,
                 path: str,
                 description: str = ""):
        self.registrar = registrar
        self.name = name
        self.variant = variant
        self.owner = owner
        self.provider = provider
        self.path = path
        self.description = description

    def __call__(self, fn: Callable[[], str]):
        if self.description == "":
            self.description = fn.__doc__
        if self.name == "":
            self.name = fn.__name__
        self.__set_query(fn())
        fn.register_resources = self.register_resources
        return fn

    def name_variant(self):
        return (self.name, self.variant)

    def pandas(self):
        """
        Returns the local source as a pandas datafame.

        Returns:
        dataframe (pandas.Dataframe): A pandas Dataframe
        """
        return pd.read_csv(self.path)

    def register_resources(
            self,
            entity: Union[str, EntityRegistrar],
            entity_column: str,
            owner: Union[str, UserRegistrar] = "",
            inference_store: Union[str, OnlineProvider, FileStoreProvider] = "",
            features: List[ColumnMapping] = None,
            labels: List[ColumnMapping] = None,
            timestamp_column: str = ""
    ):
        """
        Registers a features and/or labels that can be used in training sets or served.

        **Examples**:
        ``` py
        average_user_transaction.register_resources(
            entity=user,
            entity_column="CustomerID",
            inference_store=local,
            features=[
                {"name": <feature name>, "variant": <feature variant>, "column": <value column>, "type": "float32"}, # Column Mapping
            ],
        )
        ```

        Args:
            entity (Union[str, EntityRegistrar]): The name to reference the entity by when serving features
            entity_column (str): The name of the column in the source to be used as the entity
            owner (Union[str, UserRegistrar]): The owner of the resource(s)
            inference_store (Union[str, OnlineProvider, FileStoreProvider]): Where to store the materialized feature for serving. (Use the local provider in Localmode)
            features (List[ColumnMapping]): A list of column mappings to define the features
            labels (List[ColumnMapping]): A list of column mappings to define the labels
            timestamp_column: (str): The name of an optional timestamp column in the dataset. Will be used to match the features and labels with point-in-time correctness

        Returns:
            registrar (ResourceRegister): Registrar
        """
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
                 owner: str,
                 provider: str,
                 variant: str = "default",
                 name: str = "",
                 schedule: str = "",
                 description: str = "",
                 args: K8sArgs = None):
        self.registrar = registrar,
        self.name = name
        self.variant = variant
        self.owner = owner
        self.schedule = schedule
        self.provider = provider
        self.description = description
        self.args = args

    def __call__(self, fn: Callable[[], str]):
        if self.description == "" and fn.__doc__ is not None:
            self.description = fn.__doc__
        if self.name == "":
            self.name = fn.__name__
        self.__set_query(fn())
        fn.register_resources = self.register_resources
        fn.name_variant = self.name_variant
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
            definition=SQLTransformation(self.query, self.args),
            owner=self.owner,
            schedule=self.schedule,
            provider=self.provider,
            description=self.description,
        )

    def name_variant(self):
        return (self.name, self.variant)

    def register_resources(
            self,
            entity: Union[str, EntityRegistrar],
            entity_column: str,
            owner: Union[str, UserRegistrar] = "",
            inference_store: Union[str, OnlineProvider, FileStoreProvider] = "",
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
                 owner: str,
                 provider: str,
                 variant: str = "default",
                 name: str = "",
                 description: str = "",
                 inputs: list = [],
                 args: K8sArgs = None):
        self.registrar = registrar,
        self.name = name
        self.variant = variant
        self.owner = owner
        self.provider = provider
        self.description = description
        self.inputs = inputs
        self.args = args

    def __call__(self, fn):
        if self.description == "" and fn.__doc__ is not None:
            self.description = fn.__doc__
        if self.name == "":
            self.name = fn.__name__

        for nv in self.inputs:
            if self.name is nv[0] and self.variant is nv[1]:
                raise ValueError(f"Transformation cannot be input for itself: {self.name} {self.variant}")
        self.query = dill.dumps(fn.__code__)
        fn.register_resources = self.register_resources
        fn.name_variant = self.name_variant
        return fn

    def to_source(self) -> Source:
        return Source(
            name=self.name,
            variant=self.variant,
            definition=DFTransformation(self.query, self.inputs, self.args),
            owner=self.owner,
            provider=self.provider,
            description=self.description,
        )

    def name_variant(self):
        return (self.name, self.variant)

    def register_resources(
            self,
            entity: Union[str, EntityRegistrar],
            entity_column: str,
            owner: Union[str, UserRegistrar] = "",
            inference_store: Union[str, OnlineProvider, FileStoreProvider] = "",
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
            inference_store: Union[str, OnlineProvider, FileStoreProvider] = "",
            features: List[ColumnMapping] = None,
            labels: List[ColumnMapping] = None,
            timestamp_column: str = "",
            description: str = "",
            schedule: str = "",
    ):
        """
        Registers a features and/or labels that can be used in training sets or served.

        **Examples**:
        ``` py
        average_user_transaction.register_resources(
            entity=user,
            entity_column="CustomerID",
            inference_store=local,
            features=[
                {"name": "avg_transactions", "variant": "quickstart", "column": "TransactionAmount", "type": "float32"},
            ],
        )
        ```

        Args:
            entity (Union[str, EntityRegistrar]): The name to reference the entity by when serving features
            entity_column (str): The name of the column in the source to be used as the entity
            owner (Union[str, UserRegistrar]): The owner of the resource(s)
            inference_store (Union[str, OnlineProvider, FileStoreProvider]): Where to store the materialized feature for serving. (Use the local provider in Localmode)
            features (List[ColumnMapping]): A list of column mappings to define the features
            labels (List[ColumnMapping]): A list of column mappings to define the labels
            timestamp_column: (str): The name of an optional timestamp column in the dataset. Will be used to match the features and labels with point-in-time correctness

        Returns:
            registrar (ResourceRegister): Registrar
        """
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


class ResourceRegistrar:

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
                            resources: List = None,
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
            resources=resources,
            owner=owner,
            schedule=schedule,
            description=description,
        )

    def features(self):
        return self.__features

    def label(self):
        if isinstance(self.__labels, list):
            if len(self.__labels) > 1:
                raise ValueError("A resource used has multiple labels. A training set can only have one label")
            elif len(self.__labels) == 1:
                self.__labels = (self.__labels[0]["name"], self.__labels[0]["variant"])
            else:
                self.__labels = ()
        return self.__labels


class Registrar:
    """These functions are used to registed new resources and retrieving existing resources. Retrieved resources can be used to register additional resources. If information on these resources is needed (e.g. retrieve the names of all variants of a feature), use the [Resource Client](resource_client.md) instead.

    ``` py title="definitions.py"
    import featureform as ff

    # e.g. registering a new provider
    redis = ff.register_redis(
        name="redis-quickstart",
        host="quickstart-redis",  # The internal dns name for redis
        port=6379,
        description="A Redis deployment we created for the Featureform quickstart"
    )
    ```
    """

    def __init__(self):
        self.__state = ResourceState()
        self.__resources = []
        self.__default_owner = ""

    def add_resource(self, resource):
        self.__resources.append(resource)

    def get_resources(self):
        return self.__resources

    def register_user(self, name: str) -> UserRegistrar:
        """Register a user.

        Args:
            name (str): User to be registered.

        Returns:
            UserRegistrar: User
        """
        user = User(name)
        self.__resources.append(user)
        return UserRegistrar(self, user)

    def set_default_owner(self, user: str):
        """Set default owner.

        Args:
            user (str): User to be set as default owner of resources.
        """
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
        """Get a source. The returned object can be used to register additional resources.

        **Examples**:
        ``` py
        transactions = get_source("transactions","kaggle")
        transactions.register_resources(
            entity=user,
            entity_column="customerid",
            labels=[
                {"name": "fraudulent", "variant": "quickstart", "column": "isfraud", "type": "bool"},
            ],
        )
        ```
        Args:
            name (str): Name of source to be retrieved
            variant (str): Name of variant of source to be retrieved
            local (bool): If localmode is being used

        Returns:
            source (ColumnSourceRegistrar): Source
        """
        get = SourceReference(name=name, variant=variant, obj=None)
        self.__resources.append(get)
        if local:
            return LocalSource(self,
                               name=name,
                               owner="",
                               variant=variant,
                               provider="",
                               description="",
                               path="")
        else:
            fakeDefinition = PrimaryData(location=SQLTable(name=""))
            fakeSource = Source(name=name,
                                variant=variant,
                                definition=fakeDefinition,
                                owner="",
                                provider="",
                                description="")
            return ColumnSourceRegistrar(self, fakeSource)

    def get_local_provider(self, name="local-mode"):
        get = ProviderReference(name=name, provider_type="local", obj=None)
        self.__resources.append(get)
        fakeConfig = LocalConfig()
        fakeProvider = Provider(name=name, function="LOCAL_ONLINE", description="", team="", config=fakeConfig)
        return LocalProvider(self, fakeProvider)

    def get_redis(self, name):
        """Get a Redis provider. The returned object can be used to register additional resources.

        **Examples**:
        ``` py
        redis = get_redis("redis-quickstart")
        // Defining a new transformation source with retrieved Redis provider
        average_user_transaction.register_resources(
            entity=user,
            entity_column="user_id",
            inference_store=redis,
            features=[
                {"name": "avg_transactions", "variant": "quickstart", "column": "avg_transaction_amt", "type": "float32"},
            ],
        )
        ```
        Args:
            name (str): Name of Redis provider to be retrieved

        Returns:
            redis (OnlineProvider): Provider
        """
        get = ProviderReference(name=name, provider_type="redis", obj=None)
        self.__resources.append(get)
        fakeConfig = RedisConfig(host="", port=123, password="", db=123)
        fakeProvider = Provider(name=name, function="ONLINE", description="", team="", config=fakeConfig)
        return OnlineProvider(self, fakeProvider)

    def get_mongodb(self, name):
        """Get a MongoDB provider. The returned object can be used to register additional resources.

        **Examples**:
        ``` py
        mongodb = get_mongodb("mongodb-quickstart")
        // Defining a new transformation source with retrieved MongoDB provider
        average_user_transaction.register_resources(
            entity=user,
            entity_column="user_id",
            inference_store=mongodb,
            features=[
                {"name": "avg_transactions", "variant": "quickstart", "column": "avg_transaction_amt", "type": "float32"},
            ],
        )
        ```
        Args:
            name (str): Name of MongoDB provider to be retrieved

        Returns:
            mongodb (OnlineProvider): Provider
        """
        get = ProviderReference(name=name, provider_type="mongodb", obj=None)
        self.__resources.append(get)
        mock_config = MongoDBConfig()
        mock_provider = Provider(name=name, function="ONLINE", description="", team="", config=mock_config)
        return OnlineProvider(self, mock_provider)

    def get_blob_store(self, name):
        """Get a Azure Blob provider. The returned object can be used to register additional resources.

        **Examples**:
        ``` py
        azure_blob = get_blob_store("azure-blob-quickstart")
        // Defining a new transformation source with retrieved Azure blob provider
        average_user_transaction.register_resources(
            entity=user,
            entity_column="user_id",
            inference_store=azure_blob,
            features=[
                {"name": "avg_transactions", "variant": "quickstart", "column": "avg_transaction_amt", "type": "float32"},
            ],
        )
        ```
        Args:
            name (str): Name of Azure blob provider to be retrieved

        Returns:
            azure_blob (FileStoreProvider): Provider
        """
        get = ProviderReference(name=name, provider_type="AZURE", obj=None)
        self.__resources.append(get)
        fake_azure_config = AzureFileStoreConfig(account_name="", account_key="", container_name="", root_path="")
        fake_config = OnlineBlobConfig(store_type="AZURE", store_config=fake_azure_config.config())
        fakeProvider = Provider(name=name, function="ONLINE", description="", team="", config=fake_config)
        return FileStoreProvider(self, fakeProvider, fake_config, "AZURE")

    def get_postgres(self, name):
        """Get a Postgres provider. The returned object can be used to register additional resources.

        **Examples**:
        ``` py
        postgres = get_postgres("postgres-quickstart")
        transactions = postgres.register_table(
            name="transactions",
            variant="kaggle",
            description="Fraud Dataset From Kaggle",
            table="Transactions",  # This is the table's name in Postgres
        )
        ```
        Args:
            name (str): Name of Postgres provider to be retrieved

        Returns:
            postgres (OfflineSQLProvider): Provider
        """
        get = ProviderReference(name=name, provider_type="postgres", obj=None)
        self.__resources.append(get)
        fakeConfig = PostgresConfig(host="", port="", database="", user="", password="")
        fakeProvider = Provider(name=name, function="OFFLINE", description="", team="", config=fakeConfig)
        return OfflineSQLProvider(self, fakeProvider)

    def get_snowflake(self, name):
        """Get a Snowflake provider. The returned object can be used to register additional resources.

        **Examples**:
        ``` py
        snowflake = get_snowflake("snowflake-quickstart")
        transactions = snowflake.register_table(
            name="transactions",
            variant="kaggle",
            description="Fraud Dataset From Kaggle",
            table="Transactions",  # This is the table's name in Postgres
        )
        ```
        Args:
            name (str): Name of Snowflake provider to be retrieved

        Returns:
            snowflake (OfflineSQLProvider): Provider
        """
        get = ProviderReference(name=name, provider_type="snowflake", obj=None)
        self.__resources.append(get)
        fakeConfig = SnowflakeConfig(account="", database="", organization="", username="", password="", schema="")
        fakeProvider = Provider(name=name, function="OFFLINE", description="", team="", config=fakeConfig)
        return OfflineSQLProvider(self, fakeProvider)

    def get_redshift(self, name):
        """Get a Redshift provider. The returned object can be used to register additional resources.

        **Examples**:
        ``` py
        redshift = get_redshift("redshift-quickstart")
        transactions = redshift.register_table(
            name="transactions",
            variant="kaggle",
            description="Fraud Dataset From Kaggle",
            table="Transactions",  # This is the table's name in Postgres
        )
        ```
        Args:
            name (str): Name of Redshift provider to be retrieved

        Returns:
            redshift (OfflineSQLProvider): Provider
        """
        get = ProviderReference(name=name, provider_type="redshift", obj=None)
        self.__resources.append(get)
        fakeConfig = RedshiftConfig(host="", port="", database="", user="", password="")
        fakeProvider = Provider(name=name, function="OFFLINE", description="", team="", config=fakeConfig)
        return OfflineSQLProvider(self, fakeProvider)

    def get_bigquery(self, name):
        """Get a BigQuery provider. The returned object can be used to register additional resources.

        **Examples**:
        ``` py
        bigquery = get_bigquery("bigquery-quickstart")
        transactions = bigquery.register_table(
            name="transactions",
            variant="kaggle",
            description="Fraud Dataset From Kaggle",
            table="Transactions",  # This is the table's name in Postgres
        )
        ```
        Args:
            name (str): Name of BigQuery provider to be retrieved

        Returns:
            bigquery (OfflineSQLProvider): Provider
        """
        get = ProviderReference(name=name, provider_type="bigquery", obj=None)
        self.__resources.append(get)
        fakeConfig = BigQueryConfig(project_id="", dataset_id="", credentials_path="")
        fakeProvider = Provider(name=name, function="OFFLINE", description="", team="", config=fakeConfig)
        return OfflineSQLProvider(self, fakeProvider)

    def get_spark(self, name):
        """Get a Spark provider. The returned object can be used to register additional resources.
        **Examples**:
        ``` py
        spark = get_spark("spark-quickstart")
        transactions = spark.register_table(
            name="transactions",
            variant="kaggle",
            description="Fraud Dataset From Kaggle",
            table="Transactions",  # This is the table's name in Postgres
        )
        ```
        Args:
            name (str): Name of Spark provider to be retrieved
        Returns:
            spark (OfflineSQLProvider): Provider
        """
        get = ProviderReference(name=name, provider_type="spark", obj=None)
        self.__resources.append(get)
        fakeConfig = SparkConfig(executor_type="", executor_config={}, store_type="", store_config={})
        fakeProvider = Provider(name=name, function="OFFLINE", description="", team="", config=fakeConfig)
        return OfflineSparkProvider(self, fakeProvider)

    def get_kubernetes(self, name):
        """
        Get a k8s Azure provider. The returned object can be used to register additional resources.
        **Examples**:
        ``` py

        k8s_azure = get_kubernetes("k8s-azure-quickstart")
        transactions = k8s_azure.register_file(
            name="transactions",
            variant="kaggle",
            description="Fraud Dataset From Kaggle",
            path="path/to/blob",
        )
        ```
        Args:
            name (str): Name of k8s Azure provider to be retrieved
        Returns:
            k8s_azure (OfflineK8sProvider): Provider
        """
        get = ProviderReference(name=name, provider_type="k8s-azure", obj=None)
        self.__resources.append(get)

        fakeConfig = K8sConfig(store_type="", store_config={})
        fakeProvider = Provider(name=name, function="OFFLINE", description="", team="", config=fakeConfig)
        return OfflineK8sProvider(self, fakeProvider)

    def get_s3(self, name):
        get = ProviderReference(name=name, provider_type="S3", obj=None)
        self.__resources.append(get)

        fake_creds = AWSCredentials("id", "secret")
        fakeConfig = S3StoreConfig(bucket_path="", bucket_region="", credentials=fake_creds)
        fakeProvider = Provider(name=name, function="OFFLINE", description="", team="", config=fakeConfig)
        return OfflineK8sProvider(self, fakeProvider)

    def get_entity(self, name, local=False):
        """Get an entity. The returned object can be used to register additional resources.

        **Examples**:
        ``` py
        entity = get_entity("user")
        transactions.register_resources(
            entity=entity,
            entity_column="customerid",
            labels=[
                {"name": "fraudulent", "variant": "quickstart", "column": "isfraud", "type": "bool"},
            ],
        )
        ```
        Args:
            name (str): Name of entity to be retrieved
            local (bool): If localmode is being used

        Returns:
            entity (EntityRegistrar): Entity
        """
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
        """Register a Redis provider.

        **Examples**:
        ```
        redis = ff.register_redis(
            name="redis-quickstart",
            host="quickstart-redis",  # The internal dns name for redis
            port=6379,
            description="A Redis deployment we created for the Featureform quickstart"
        )
        ```
        Args:
            name (str): Name of Redis provider to be registered
            description (str): Description of Redis provider to be registered
            team (str): Name of team
            host (str): Internal DNS name for Redis
            port (int): Redis port
            password (str): Redis password
            db (str): Redis database

        Returns:
            redis (OnlineProvider): Provider
        """
        config = RedisConfig(host=host, port=port, password=password, db=db)
        provider = Provider(name=name,
                            function="ONLINE",
                            description=description,
                            team=team,
                            config=config)
        self.__resources.append(provider)
        return OnlineProvider(self, provider)

    def register_blob_store(self,
                            name: str,
                            account_name: str,
                            account_key: str,
                            container_name: str,
                            root_path: str,
                            description: str = "",
                            team: str = "", ):

        """Register an azure blob store provider.

        This has the functionality of an online store and can be used as a parameter
        to a k8s or spark provider

        **Examples**:
        ```
        blob = ff.register_blob_store(
            name="azure-quickstart",
            container_name="my_company_container"
            root_path="custom/path/in/container"
            account_name=<azure_account_name>
            account_key=<azure_account_key> 
            description="An azure blob store provider to store offline and inference data"
        )
        ```
        Args:
            name (str): Name of Azure blob store to be registered
            container_name (str): Azure container name
            root_path (str): custom path in container to store data
            description (str): Description of Redis provider to be registered
            team (str): team with permission to this storage layer
            account_name (str): Azure account name
            account_key (str): Secret azure account key
            config (AzureConfig): an azure config object (can be used in place of container name and account name)
        Returns:
            blob (StorageProvider): Provider
                has all the functionality of OnlineProvider
        """

        azure_config = AzureFileStoreConfig(account_name=account_name, account_key=account_key,
                                            container_name=container_name, root_path=root_path)
        config = OnlineBlobConfig(store_type="AZURE", store_config=azure_config.config())

        provider = Provider(name=name,
                            function="ONLINE",
                            description=description,
                            team=team,
                            config=config)
        self.__resources.append(provider)
        return FileStoreProvider(self, provider, azure_config, "AZURE")
    
    def register_s3(self,
                    name: str,
                    credentials: AWSCredentials,
                    bucket_path: str,
                    bucket_region: str,
                    description: str = "",
                    team: str = "", ):
        """Register a S3 store provider.

        This has the functionality of an offline store and can be used as a parameter
        to a k8s or spark provider

        **Examples**:
        ```
        s3 = ff.register_s3(
            name="s3-quickstart",
            credentials=aws_creds,
            bucket_path="bucket_name/path",
            bucket_region=<bucket_region>,
            description="An s3 store provider to store offline"
        )
        ```
        Args:
            name (str): Name of S3 store to be registered
            credentials (AWSCredentials): AWS credentials to access the bucket
            bucket_path (str): custom path including the bucket name
            bucket_region (str): aws region the bucket is located in
            description (str): Description of Redis provider to be registered
            team (str): team with permission to this storage layer
        Returns:
            s3 (FileStoreProvider): Provider
                has all the functionality of OfflineProvider
        """

        s3_config = S3StoreConfig(bucket_path=bucket_path, bucket_region=bucket_region, credentials=credentials)

        provider = Provider(name=name,
                            function="OFFLINE",
                            description=description,
                            team=team,
                            config=s3_config)
        self.__resources.append(provider)
        return FileStoreProvider(self, provider, s3_config, s3_config.type())

    def register_firestore(self,
                           name: str,
                           collection: str,
                           project_id: str,
                           credentials_path: str,
                           description: str = "",
                           team: str = "",
                           ):
        """Register a Firestore provider.

        **Examples**:
        ```
        firestore = ff.register_firestore(
            name="firestore-quickstart",
            description="A Firestore deployment we created for the Featureform quickstart",
            project_id="quickstart-project",
            collection="quickstart-collection",
        )
        ```
        Args:
            name (str): Name of Firestore provider to be registered
            description (str): Description of Firestore provider to be registered
            team (str): Name of team
            project_id (str): The Project name in GCP
            collection (str): The Collection name in Firestore under the given project ID
            credentials_path (str): A path to a Google Credentials file with access permissions for Firestore

        Returns:
            firestore (OfflineSQLProvider): Provider
        """
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
        config = CassandraConfig(host=host, port=port, username=username, password=password, keyspace=keyspace,
                                 consistency=consistency, replication=replication)
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

    def register_mongodb(self,
                         name: str,
                         description: str = "",
                         team: str = "",
                         username: str = None,
                         password: str = None,
                         database: str = None,
                         host: str = None,
                         port: str = None,
                         throughput: int = 1000

                         ):
        """Register a MongoDB provider.

        **Examples**:
        ```
        mongodb = ff.register_mongodb(
            name="mongodb-quickstart",
            description="A MongoDB deployment",
            team="myteam"
            username="my_username",
            password="myPassword",
            database="featureform_database"
            host="my-mongodb.host.com",
            port="10225"
            throughput=10000
        )
        ```
        Args:
            name (str): Name of MongoDB provider to be registered
            description (str): Description of MongoDB provider to be registered
            team (str): Name of team
            username (str): MongoDB username
            password (str): MongoDB password
            database (str): MongoDB database
            host (str): MongoDB hostname
            port (str): MongoDB port
            throughput (int): The maximum RU limit for autoscaling

        Returns:
            mongodb (OnlineProvider): Provider
        """
        config = MongoDBConfig(username=username, password=password, host=host, port=port, database=database,
                               throughput=throughput)
        provider = Provider(name=name,
                            function="ONLINE",
                            description=description,
                            team=team,
                            config=config)
        self.__resources.append(provider)
        return OnlineProvider(self, provider)

    def register_snowflake_legacy(
                self,
                name: str,
                username: str,
                password: str,
                account_locator: str,
                database: str,
                schema: str = "PUBLIC",
                description: str = "",
                team: str = "",
                warehouse: str = "",
                role: str = "",
    ):
        """Register a Snowflake provider using legacy credentials.

        **Examples**:
        ```
        snowflake = ff.register_snowflake_legacy(
            name="snowflake-quickstart",
            username="snowflake",
            password="password", #pragma: allowlist secret
            account_locator="account-locator",
            database="snowflake",
            schema="PUBLIC",
            description="A Snowflake deployment we created for the Featureform quickstart"
        )
        ```
        Args:
            name (str): Name of Snowflake provider to be registered
            username (str): Username
            password (str): Password
            account_locator (str): Account Locator
            database (str): Database
            schema (str): Schema
            description (str): Description of Snowflake provider to be registered
            team (str): Name of team
            warehouse (str): Specifies the virtual warehouse to use by default for queries, loading, etc.
            role (str): Specifies the role to use by default for accessing Snowflake objects in the client session

        Returns:
            snowflake (OfflineSQLProvider): Provider
        """
        config = SnowflakeConfig(account_locator=account_locator,
                                 database=database,
                                 username=username,
                                 password=password,
                                 schema=schema,
                                 warehouse=warehouse,
                                 role=role)
        provider = Provider(name=name,
                            function="OFFLINE",
                            description=description,
                            team=team,
                            config=config)
        self.__resources.append(provider)
        return OfflineSQLProvider(self, provider)

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
            warehouse: str = "",
            role: str = "",


    ):
        """Register a Snowflake provider.

        **Examples**:
        ```
        snowflake = ff.register_snowflake(
            name="snowflake-quickstart",
            username="snowflake",
            password="password", #pragma: allowlist secret
            account="account",
            organization="organization",
            database="snowflake",
            schema="PUBLIC",
            description="A Snowflake deployment we created for the Featureform quickstart"
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
            warehouse (str): Specifies the virtual warehouse to use by default for queries, loading, etc.
            role (str): Specifies the role to use by default for accessing Snowflake objects in the client session

        Returns:
            snowflake (OfflineSQLProvider): Provider
        """
        config = SnowflakeConfig(account=account,
                                 database=database,
                                 organization=organization,
                                 username=username,
                                 password=password,
                                 schema=schema,
                                 warehouse=warehouse,
                                 role=role)
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
                          port: str = "5432",
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
            password="password", #pragma: allowlist secret
            database="postgres"
        )
        ```
        Args:
            name (str): Name of Postgres provider to be registered
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
            password="password", #pragma: allowlist secret
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

    def register_bigquery(self,
                          name: str,
                          description: str = "",
                          team: str = "",
                          project_id: str = "",
                          dataset_id: str = "",
                          credentials_path: str = ""):
        """Register a BigQuery provider.

        **Examples**:
        ```
        bigquery = ff.register_bigquery(
            name="bigquery-quickstart",
            description="A BigQuery deployment we created for the Featureform quickstart",
            project_id="quickstart-project",
            dataset_id="quickstart-dataset",
        )
        ```
        Args:
            name (str): Name of BigQuery provider to be registered
            description (str): Description of BigQuery provider to be registered
            team (str): Name of team
            project_id (str): The Project name in GCP
            dataset_id (str): The Dataset name in GCP under the Project Id
            credentials_path (str): A path to a Google Credentials file with access permissions for BigQuery

        Returns:
            bigquery (OfflineSQLProvider): Provider
        """
        config = BigQueryConfig(project_id=project_id,
                                dataset_id=dataset_id,
                                credentials_path=credentials_path, )
        provider = Provider(name=name,
                            function="OFFLINE",
                            description=description,
                            team=team,
                            config=config)
        self.__resources.append(provider)
        return OfflineSQLProvider(self, provider)

    def register_spark(self,
                       name: str,
                       executor: ExecutorCredentials,
                       filestore: FileStoreProvider,
                       description: str = "",
                       team: str = "",
                       ):
        """Register a Spark on Executor provider.
        **Examples**:
        ```
        spark = ff.register_spark(
            name="spark-quickstart",
            description="A Spark deployment we created for the Featureform quickstart",
            team="featureform-team",
            executor=databricks,
            filestore=azure_blob_store
        )
        ```
        Args:
            name (str): Name of Spark provider to be registered
            executor (ExecutorCredentials): an Executor Provider used for the compute power
            filestore: (FileStoreProvider): a FileStoreProvider used for storage of data
            description (str): Description of Spark provider to be registered
            team (str): Name of team

        Returns:
            spark (OfflineSparkProvider): Provider
        """

        config = SparkConfig(
            executor_type=executor.type(),
            executor_config=executor.config(),
            store_type=filestore.store_type(),
            store_config=filestore.config())

        provider = Provider(name=name,
                            function="OFFLINE",
                            description=description,
                            team=team,
                            config=config)
        self.__resources.append(provider)
        return OfflineSparkProvider(self, provider)

    def register_k8s(self,
                     name: str,
                     store: FileStoreProvider,
                     description: str = "",
                     team: str = "",
                     docker_image: str = ""
                     ):
        """
        Register an offline store provider to run on featureform's own k8s deployment
        
        Args:
            name (str): Name of provider
            store (FileStoreProvider): Reference to registered file store provider
            description (str): Description of primary data to be registered
            team (str): A string parameter describing the team that owns the provider
            docker_image (str): A custom docker image using the base image featureformcom/k8s_runner
        **Examples**:
        ```
        k8s = ff.register_k8s(
            name="k8s",
            description="Native featureform kubernetes compute",
            store=azure_blob,
            team="featureform-team",
            docker_image="my-repo/image:version"
        )
        ```
        """
        config = K8sConfig(
            store_type=store.store_type(),
            store_config=store.config(),
            docker_image=docker_image
        )

        provider = Provider(name=name,
                            function="OFFLINE",
                            description=description,
                            team=team,
                            config=config)
        self.__resources.append(provider)
        return OfflineK8sProvider(self, provider)

    def register_local(self):
        """Register a Local provider.

        **Examples**:
        ```
            local = register_local()
        ```
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
                                    schedule: str = "",
                                    args: K8sArgs = None
                                    ):
        """Register a SQL transformation source.

        Args:
            name (str): Name of source
            variant (str): Name of variant
            query (str): SQL query
            provider (Union[str, OfflineProvider]): Provider
            owner (Union[str, UserRegistrar]): Owner
            description (str): Description of primary data to be registered
            schedule (str): Kubernetes CronJob schedule string ("* * * * *")
            args (K8sArgs): Additional transformation arguments

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
            definition=SQLTransformation(query, args),
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
                           description: str = "",
                           args: K8sArgs = None
                           ):
        """SQL transformation decorator.

        Args:
            variant (str): Name of variant
            provider (Union[str, OfflineProvider]): Provider
            name (str): Name of source
            schedule (str): Kubernetes CronJob schedule string ("* * * * *")
            owner (Union[str, UserRegistrar]): Owner
            description (str): Description of SQL transformation
            args (K8sArgs): Additional transformation arguments

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
            args=args,
        )
        self.__resources.append(decorator)
        return decorator

    def register_df_transformation(self,
                                   name: str,
                                   query: str,
                                   provider: Union[str, OfflineProvider],
                                   variant: str = "default",
                                   owner: Union[str, UserRegistrar] = "",
                                   description: str = "",
                                   inputs: list = [],
                                   schedule: str = "",
                                   args: K8sArgs = None
                                   ):
        """Register a Dataframe transformation source.

        Args:
            name (str): Name of source
            variant (str): Name of variant
            query (str): SQL query
            provider (Union[str, OfflineProvider]): Provider
            name (str): Name of source
            owner (Union[str, UserRegistrar]): Owner
            description (str): Description of SQL transformation
            inputs (list): Inputs to transformation
            schedule (str): Kubernetes CronJob schedule string ("* * * * *")
            args (K8sArgs): Additional transformation arguments

        Returns:
            source (ColumnSourceRegistrar): Source
        """
        if not isinstance(owner, str):
            owner = owner.name()
        if owner == "":
            owner = self.must_get_default_owner()
        if not isinstance(provider, str):
            provider = provider.name()
        for i, nv in enumerate(inputs):
            if not isinstance(nv, tuple):
                inputs[i] = nv.name_variant()
        source = Source(
            name=name,
            variant=variant,
            definition=DFTransformation(query, inputs, args),
            owner=owner,
            schedule=schedule,
            provider=provider,
            description=description,
        )
        self.__resources.append(source)
        return ColumnSourceRegistrar(self, source)

    def df_transformation(self,
                          provider: Union[str, OfflineProvider],
                          variant: str = "default",
                          name: str = "",
                          owner: Union[str, UserRegistrar] = "",
                          description: str = "",
                          inputs: list = [],
                          args: K8sArgs = None
                          ):
        """Dataframe transformation decorator.

        Args:
            variant (str): Name of variant
            provider (Union[str, OfflineProvider]): Provider
            name (str): Name of source
            owner (Union[str, UserRegistrar]): Owner
            description (str): Description of SQL transformation
            inputs (list): Inputs to transformation
            args (K8sArgs): Additional transformation arguments

        Returns:
            decorator (DFTransformationDecorator): decorator
        """

        if not isinstance(owner, str):
            owner = owner.name()
        if owner == "":
            owner = self.must_get_default_owner()
        if not isinstance(provider, str):
            provider = provider.name()
        for i, nv in enumerate(inputs):
            if not isinstance(nv, tuple):
                inputs[i] = nv.name_variant()
        decorator = DFTransformationDecorator(
            registrar=self,
            name=name,
            variant=variant,
            provider=provider,
            owner=owner,
            description=description,
            inputs=inputs,
            args=args
        )
        self.__resources.append(decorator)
        return decorator

    def state(self):
        for resource in self.__resources:
            try:
                if isinstance(resource, SQLTransformationDecorator) or isinstance(resource, DFTransformationDecorator):
                    resource = resource.to_source()
                self.__state.add(resource)
            except ResourceRedefinedError:
                raise
            except Exception as e:
                raise Exception(f"Could not add apply {resource.name} ({resource.variant}): {e}")
        self.__resources = []
        return self.__state

    def clear_state(self):
        self.__state = ResourceState()

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
            inference_store: Union[str, OnlineProvider, FileStoreProvider] = "",
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

        if type(inference_store) == FileStoreProvider and inference_store.store_type() in NON_INFERENCE_STORES:
            raise Exception(f"cannot use '{inference_store.store_type()}' as an inference store.")

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
            variant = feature.get("variant", "default")
            desc = feature.get("description", "")
            resource = Feature(
                name=feature["name"],
                variant=variant,
                source=source,
                value_type=feature["type"],
                entity=entity,
                owner=owner,
                provider=inference_store,
                description=desc,
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
            variant = label.get("variant", "default")
            desc = label.get("description", "")
            resource = Label(
                name=label["name"],
                variant=variant,
                source=source,
                value_type=label["type"],
                entity=entity,
                owner=owner,
                provider=inference_store,
                description=desc,
                location=ResourceColumnMapping(
                    entity=entity_column,
                    value=label["column"],
                    timestamp=timestamp_column,
                ),
            )
            self.__resources.append(resource)
            label_resources.append(resource)
        return ResourceRegistrar(self, features, labels)

    def __get_feature_nv(self, features):
        feature_nv_list = []
        feature_lags = []
        for feature in features:
            if isinstance(feature, str):
                feature_nv = (feature, "default")
                feature_nv_list.append(feature_nv)
            elif isinstance(feature, dict):
                lag = feature.get("lag")
                if lag:
                    required_lag_keys = set(["lag", "feature", "variant"])
                    received_lag_keys = set(feature.keys())
                    if required_lag_keys.intersection(received_lag_keys) != required_lag_keys:
                        raise ValueError(
                            f"feature lags require 'lag', 'feature', 'variant' fields. Received: {feature.keys()}")

                    if not isinstance(lag, timedelta):
                        raise ValueError(
                            f"the lag, '{lag}', needs to be of type 'datetime.timedelta'. Received: {type(lag)}.")

                    feature_name_variant = (feature["feature"], feature["variant"])
                    if feature_name_variant not in feature_nv_list:
                        feature_nv_list.append(feature_name_variant)

                    lag_name = f"{feature['feature']}_{feature['variant']}_lag_{lag}"
                    sanitized_lag_name = lag_name.replace(" ", "").replace(",", "_").replace(":", "_")
                    feature["name"] = feature.get("name", sanitized_lag_name)

                    feature_lags.append(feature)
                else:
                    feature_nv = (feature["name"], feature["variant"])
                    feature_nv_list.append(feature_nv)
            elif isinstance(feature, list):
                feature_nv, feature_lags_list = self.__get_feature_nv(feature)
                if len(feature_nv) != 0:
                    feature_nv_list.extend(feature_nv)

                if len(feature_lags_list) != 0:
                    feature_lags.extend(feature_lags_list)
            else:
                feature_nv_list.append(feature)

        return feature_nv_list, feature_lags

    def register_training_set(self,
                              name: str,
                              variant: str = "default",
                              features: list = [],
                              label: NameVariant = (),
                              resources: list = [],
                              owner: Union[str, UserRegistrar] = "",
                              description: str = "",
                              schedule: str = ""):
        """Register a training set.

        Args:
            name (str): Name of training set to be registered
            variant (str): Name of variant to be registered
            label (NameVariant): Label of training set
            features (List[NameVariant]): Features of training set
            resources (List[Resource]): A list of previously registered resources
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

        if isinstance(features, tuple):
            raise ValueError("Features must be entered as a list")

        if isinstance(label, list):
            raise ValueError("Label must be entered as a tuple")

        for resource in resources:
            features += resource.features()
            resource_label = resource.label()
            # label == () if it is NOT manually entered
            if label == ():
                label = resource_label
            # Elif: If label was updated to store resource_label it will not check the following elif
            elif resource_label != ():
                raise ValueError("A training set can only have one label")

        if isinstance(label, str):
            label = (label, "default")

        features, feature_lags = self.__get_feature_nv(features)

        if label == ():
            raise ValueError("Label must be set")
        if features == []:
            raise ValueError("A training-set must have atleast one feature")

        resource = TrainingSet(
            name=name,
            variant=variant,
            description=description,
            owner=owner,
            schedule=schedule,
            label=label,
            features=features,
            feature_lags=feature_lags
        )
        self.__resources.append(resource)


class ResourceClient(Registrar):
    """The resource client is used to retrieve information on specific resources (entities, providers, features, labels, training sets, models, users). If retrieved resources are needed to register additional resources (e.g. registering a feature from a source), use the [Client](client.md) functions instead.

    **Using the Resource Client:**
    ``` py title="definitions.py"
    import featureform as ff
    from featureform import ResourceClient

    rc = ResourceClient("localhost:8000")

    # example query:
    redis = rc.get_provider("redis-quickstart")
    ```
    """

    def __init__(self, host=None, local=False, insecure=False, cert_path=None, dry_run=False):
        """Initialise a Resource Client object.

        Args:
            host (str): The hostname of the Featureform instance. Exclude if using Localmode.
            local (bool): True if using Localmode.
            insecure (bool): True if connecting to an insecure Featureform endpoint. False if using a self-signed or public TLS certificate
            cert_path (str): The path to a public certificate if using a self-signed certificate.
        """
        super().__init__()
        self._dry_run = dry_run
        self._stub = None
        self.local = local

        if dry_run:
            return

        if local and host:
            raise ValueError("Cannot be local and have a host")
        elif not local:
            host = host or os.getenv('FEATUREFORM_HOST')
            if host is None:
                raise RuntimeError(
                    'If not in local mode then `host` must be passed or the environment'
                    ' variable FEATUREFORM_HOST must be set.'
                )
            if insecure:
                channel = insecure_channel(host)
            else:
                channel = secure_channel(host, cert_path)
            self._stub = ff_grpc.ApiStub(channel)

    def apply(self):
        """Apply all definitions, creating and retrieving all specified resources.
        """

        if self._dry_run:
            print(state().sorted_list())
            return

        if self.local:
            state().create_all_local()
        else:
            state().create_all(self._stub)

    def get_user(self, name, local=False):
        """Get a user. Prints out name of user, and all resources associated with the user.

        **Examples:**

        ``` py title="Input"
        featureformer = rc.get_user("featureformer")
        ```

        ``` json title="Output"
        // get_user prints out formatted information on user
        USER NAME:                     featureformer
        -----------------------------------------------

        NAME                           VARIANT                        TYPE
        avg_transactions               quickstart                     feature
        fraudulent                     quickstart                     label
        fraud_training                 quickstart                     training set
        transactions                   kaggle                         source
        average_user_transaction       quickstart                     source
        -----------------------------------------------
        ```

        ``` py title="Input"
        print(featureformer)
        ```

        ``` json title="Output"
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
        """Get an entity. Prints out information on entity, and all resources associated with the entity.

        **Examples:**

        ``` py title="Input"
        entity = rc.get_entity("user")
        ```

        ``` json title="Output"
        // get_entity prints out formatted information on entity

        ENTITY NAME:                   user
        STATUS:                        NO_STATUS
        -----------------------------------------------

        NAME                           VARIANT                        TYPE
        avg_transactions               quickstart                     feature
        fraudulent                     quickstart                     label
        fraud_training                 quickstart                     training set
        -----------------------------------------------
        ```

        ``` py title="Input"
        print(postgres)
        ```

        ``` json title="Output"
        // get_entity returns the Entity object

        name: "user"
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
        ```
        """
        if local:
            return get_entity_info_local(name)
        return get_entity_info(self._stub, name)

    def get_model(self, name):
        """Get a model. Prints out information on model, and all resources associated with the model.

        Args:
            name (str): Name of model to be retrieved

        Returns:
            model (Model): Model
        """
        return get_resource_info(self._stub, "model", name)

    def get_provider(self, name, local=False):
        """Get a provider. Prints out information on provider, and all resources associated with the provider.

        **Examples:**

        ``` py title="Input"
        postgres = rc.get_provider("postgres-quickstart")
        ```

        ``` json title="Output"
        // get_provider prints out formatted information on provider

        NAME:                          postgres-quickstart
        DESCRIPTION:                   A Postgres deployment we created for the Featureform quickstart
        TYPE:                          POSTGRES_OFFLINE
        SOFTWARE:                      postgres
        STATUS:                        NO_STATUS
        -----------------------------------------------
        SOURCES:
        NAME                           VARIANT
        transactions                   kaggle
        average_user_transaction       quickstart
        -----------------------------------------------
        FEATURES:
        NAME                           VARIANT
        -----------------------------------------------
        LABELS:
        NAME                           VARIANT
        fraudulent                     quickstart
        -----------------------------------------------
        TRAINING SETS:
        NAME                           VARIANT
        fraud_training                 quickstart
        -----------------------------------------------
        ```

        ``` py title="Input"
        print(postgres)
        ```

        ``` json title="Output"
        // get_provider returns the Provider object

        name: "postgres-quickstart"
        description: "A Postgres deployment we created for the Featureform quickstart"
        type: "POSTGRES_OFFLINE"
        software: "postgres"
        serialized_config: "{\"Host\": \"quickstart-postgres\",
                            \"Port\": \"5432\",
                            \"Username\": \"postgres\",
                            \"Password\": \"password\",
                            \"Database\": \"postgres\"}"
        sources {
        name: "transactions"
        variant: "kaggle"
        }
        sources {
        name: "average_user_transaction"
        variant: "quickstart"
        }
        trainingsets {
        name: "fraud_training"
        variant: "quickstart"
        }
        labels {
        name: "fraudulent"
        variant: "quickstart"
        }
        ```

        Args:
            name (str): Name of provider to be retrieved

        Returns:
            provider (Provider): Provider
        """
        if local:
            return get_provider_info_local(name)
        return get_provider_info(self._stub, name)

    def get_feature(self, name, variant):
        name_variant = metadata_pb2.NameVariant(name=name, variant=variant)
        feature = None
        for x in self._stub.GetFeatureVariants(iter([name_variant])):
            feature = x
            break

        return Feature(
            name=feature.name,
            variant=feature.variant,
            source=(feature.source.name, feature.source.variant),
            value_type=feature.type,
            entity=feature.entity,
            owner=feature.owner,
            provider=feature.provider,
            location=ResourceColumnMapping("", "", ""),
            description=feature.description,
            status=feature.status.Status._enum_type.values[feature.status.status].name
        )

    def print_feature(self, name, variant=None, local=False):
        """Get a feature. Prints out information on feature, and all variants associated with the feature. If variant is included, print information on that specific variant and all resources associated with it.

        **Examples:**

        ``` py title="Input"
        avg_transactions = rc.get_feature("avg_transactions")
        ```

        ``` json title="Output"
        // get_feature prints out formatted information on feature

        NAME:                          avg_transactions
        STATUS:                        NO_STATUS
        -----------------------------------------------
        VARIANTS:
        quickstart                     default
        -----------------------------------------------
        ```

        ``` py title="Input"
        print(avg_transactions)
        ```

        ``` json title="Output"
        // get_feature returns the Feature object

        name: "avg_transactions"
        default_variant: "quickstart"
        variants: "quickstart"
        ```

        ``` py title="Input"
        avg_transactions_variant = ff.get_feature("avg_transactions", "quickstart")
        ```

        ``` json title="Output"
        // get_feature with variant provided prints out formatted information on feature variant

        NAME:                          avg_transactions
        VARIANT:                       quickstart
        TYPE:                          float32
        ENTITY:                        user
        OWNER:                         featureformer
        PROVIDER:                      redis-quickstart
        STATUS:                        NO_STATUS
        -----------------------------------------------
        SOURCE:
        NAME                           VARIANT
        average_user_transaction       quickstart
        -----------------------------------------------
        TRAINING SETS:
        NAME                           VARIANT
        fraud_training                 quickstart
        -----------------------------------------------
        ```

        ``` py title="Input"
        print(avg_transactions_variant)
        ```

        ``` json title="Output"
        // get_feature returns the FeatureVariant object

        name: "avg_transactions"
        variant: "quickstart"
        source {
        name: "average_user_transaction"
        variant: "quickstart"
        }
        type: "float32"
        entity: "user"
        created {
        seconds: 1658168552
        nanos: 142461900
        }
        owner: "featureformer"
        provider: "redis-quickstart"
        trainingsets {
        name: "fraud_training"
        variant: "quickstart"
        }
        columns {
        entity: "user_id"
        value: "avg_transaction_amt"
        }
        ```

        Args:
            name (str): Name of feature to be retrieved
            variant (str): Name of variant of feature

        Returns:
            feature (Union[Feature, FeatureVariant]): Feature or FeatureVariant
        """
        if local:
            if not variant:
                return get_resource_info_local("feature", name)
            return get_feature_variant_info_local(name, variant)
        if not variant:
            return get_resource_info(self._stub, "feature", name)
        return get_feature_variant_info(self._stub, name, variant)

    def get_label(self, name, variant):
        name_variant = metadata_pb2.NameVariant(name=name, variant=variant)
        label = None
        for x in self._stub.GetLabelVariants(iter([name_variant])):
            label = x
            break

        return Label(
            name=label.name,
            variant=label.variant,
            source=(label.source.name, label.source.variant),
            value_type=label.type,
            entity=label.entity,
            owner=label.owner,
            provider=label.provider,
            location=ResourceColumnMapping("", "", ""),
            description=label.description,
            status=label.status.Status._enum_type.values[label.status.status].name
        )

    def print_label(self, name, variant=None, local=False):
        """Get a label. Prints out information on label, and all variants associated with the label. If variant is included, print information on that specific variant and all resources associated with it.

        **Examples:**

        ``` py title="Input"
        fraudulent = rc.get_label("fraudulent")
        ```

        ``` json title="Output"
        // get_label prints out formatted information on label

        NAME:                          fraudulent
        STATUS:                        NO_STATUS
        -----------------------------------------------
        VARIANTS:
        quickstart                     default
        -----------------------------------------------
        ```

        ``` py title="Input"
        print(fraudulent)
        ```

        ``` json title="Output"
        // get_label returns the Label object

        name: "fraudulent"
        default_variant: "quickstart"
        variants: "quickstart"
        ```

        ``` py title="Input"
        fraudulent_variant = ff.get_label("fraudulent", "quickstart")
        ```

        ``` json title="Output"
        // get_label with variant provided prints out formatted information on label variant

        NAME:                          fraudulent
        VARIANT:                       quickstart
        TYPE:                          bool
        ENTITY:                        user
        OWNER:                         featureformer
        PROVIDER:                      postgres-quickstart
        STATUS:                        NO_STATUS
        -----------------------------------------------
        SOURCE:
        NAME                           VARIANT
        transactions                   kaggle
        -----------------------------------------------
        TRAINING SETS:
        NAME                           VARIANT
        fraud_training                 quickstart
        -----------------------------------------------
        ```

        ``` py title="Input"
        print(fraudulent_variant)
        ```

        ``` json title="Output"
        // get_label returns the LabelVariant object

        name: "fraudulent"
        variant: "quickstart"
        type: "bool"
        source {
        name: "transactions"
        variant: "kaggle"
        }
        entity: "user"
        created {
        seconds: 1658168552
        nanos: 154924300
        }
        owner: "featureformer"
        provider: "postgres-quickstart"
        trainingsets {
        name: "fraud_training"
        variant: "quickstart"
        }
        columns {
        entity: "customerid"
        value: "isfraud"
        }
        ```

        Args:
            name (str): Name of label to be retrieved
            variant (str): Name of variant of label

        Returns:
            label (Union[label, LabelVariant]): Label or LabelVariant
        """
        if local:
            if not variant:
                return get_resource_info_local("label", name)
            return get_label_variant_info_local(name, variant)
        if not variant:
            return get_resource_info(self._stub, "label", name)
        return get_label_variant_info(self._stub, name, variant)

    def get_training_set(self, name, variant):
        name_variant = metadata_pb2.NameVariant(name=name, variant=variant)
        ts = None
        for x in self._stub.GetTrainingSetVariants(iter([name_variant])):
            ts = x
            break

        return TrainingSet(
            name=ts.name,
            variant=ts.variant,
            owner=ts.owner,
            description=ts.description,
            status=ts.status.Status._enum_type.values[ts.status.status].name,
            label=(ts.label.name, ts.label.variant),
            features=[(f.name, f.variant) for f in ts.features],
            feature_lags=[],
            provider=ts.provider,
        )

    def print_training_set(self, name, variant=None, local=False):
        """Get a training set. Prints out information on training set, and all variants associated with the training set. If variant is included, print information on that specific variant and all resources associated with it.

        **Examples:**

        ``` py title="Input"
        fraud_training = rc.get_training_set("fraud_training")
        ```

        ``` json title="Output"
        // get_training_set prints out formatted information on training set

        NAME:                          fraud_training
        STATUS:                        NO_STATUS
        -----------------------------------------------
        VARIANTS:
        quickstart                     default
        -----------------------------------------------
        ```

        ``` py title="Input"
        print(fraud_training)
        ```

        ``` json title="Output"
        // get_training_set returns the TrainingSet object

        name: "fraud_training"
        default_variant: "quickstart"
        variants: "quickstart"
        ```

        ``` py title="Input"
        fraudulent_variant = ff.get_training set("fraudulent", "quickstart")
        ```

        ``` json title="Output"
        // get_training_set with variant provided prints out formatted information on training set variant

        NAME:                          fraud_training
        VARIANT:                       quickstart
        OWNER:                         featureformer
        PROVIDER:                      postgres-quickstart
        STATUS:                        NO_STATUS
        -----------------------------------------------
        LABEL:
        NAME                           VARIANT
        fraudulent                     quickstart
        -----------------------------------------------
        FEATURES:
        NAME                           VARIANT
        avg_transactions               quickstart
        -----------------------------------------------
        ```

        ``` py title="Input"
        print(fraudulent_variant)
        ```

        ``` json title="Output"
        // get_training_set returns the TrainingSetVariant object

        name: "fraud_training"
        variant: "quickstart"
        owner: "featureformer"
        created {
        seconds: 1658168552
        nanos: 157934800
        }
        provider: "postgres-quickstart"
        features {
        name: "avg_transactions"
        variant: "quickstart"
        }
        label {
        name: "fraudulent"
        variant: "quickstart"
        }
        ```

        Args:
            name (str): Name of training set to be retrieved
            variant (str): Name of variant of training set

        Returns:
            training_set (Union[TrainingSet, TrainingSetVariant]): TrainingSet or TrainingSetVariant
        """
        if local:
            if not variant:
                return get_resource_info_local("training-set", name)
            return get_training_set_variant_info_local(name, variant)
        if not variant:
            return get_resource_info(self._stub, "training-set", name)
        return get_training_set_variant_info(self._stub, name, variant)

    def get_source(self, name, variant):
        name_variant = metadata_pb2.NameVariant(name=name, variant=variant)
        source = None
        for x in self._stub.GetSourceVariants(iter([name_variant])):
            source = x
            break

        definition = self._get_source_definition(source)

        return Source(
            name=source.name,
            definition=definition,
            owner=source.owner,
            provider=source.provider,
            description=source.description,
            variant=source.variant,
            status=source.status.Status._enum_type.values[source.status.status].name,
        )

    def _get_source_definition(self, source):
        if source.primaryData.table.name:
            return PrimaryData(
                Location(source.primaryData.table.name)
            )
        elif source.transformation:
            return self._get_transformation_definition(source)
        else:
            raise Exception(f"Invalid source type {source}")

    def _get_transformation_definition(self, source):
        if source.transformation.DFTransformation.query != bytes():
            transformation = source.transformation.DFTransformation
            return DFTransformation(
                query=transformation.query,
                inputs=[(input.name, input.variant) for input in transformation.inputs]
            )
        elif source.transformation.SQLTransformation.query != "":
            return SQLTransformation(
                source.transformation.SQLTransformation.query
            )
        else:
            raise Exception(f"Invalid transformation type {source}")

    def print_source(self, name, variant=None, local=False):
        """Get a source. Prints out information on source, and all variants associated with the source. If variant is included, print information on that specific variant and all resources associated with it.

        **Examples:**

        ``` py title="Input"
        transactions = rc.get_transactions("transactions")
        ```

        ``` json title="Output"
        // get_source prints out formatted information on source

        NAME:                          transactions
        STATUS:                        NO_STATUS
        -----------------------------------------------
        VARIANTS:
        kaggle                         default
        -----------------------------------------------
        ```

        ``` py title="Input"
        print(transactions)
        ```

        ``` json title="Output"
        // get_source returns the Source object

        name: "transactions"
        default_variant: "kaggle"
        variants: "kaggle"
        ```

        ``` py title="Input"
        transactions_variant = rc.get_source("transactions", "kaggle")
        ```

        ``` json title="Output"
        // get_source with variant provided prints out formatted information on source variant

        NAME:                          transactions
        VARIANT:                       kaggle
        OWNER:                         featureformer
        DESCRIPTION:                   Fraud Dataset From Kaggle
        PROVIDER:                      postgres-quickstart
        STATUS:                        NO_STATUS
        -----------------------------------------------
        DEFINITION:
        TRANSFORMATION

        -----------------------------------------------
        SOURCES
        NAME                           VARIANT
        -----------------------------------------------
        PRIMARY DATA
        Transactions
        FEATURES:
        NAME                           VARIANT
        -----------------------------------------------
        LABELS:
        NAME                           VARIANT
        fraudulent                     quickstart
        -----------------------------------------------
        TRAINING SETS:
        NAME                           VARIANT
        fraud_training                 quickstart
        -----------------------------------------------
        ```

        ``` py title="Input"
        print(transactions_variant)
        ```

        ``` json title="Output"
        // get_source returns the SourceVariant object

        name: "transactions"
        variant: "kaggle"
        owner: "featureformer"
        description: "Fraud Dataset From Kaggle"
        provider: "postgres-quickstart"
        created {
        seconds: 1658168552
        nanos: 128768000
        }
        trainingsets {
        name: "fraud_training"
        variant: "quickstart"
        }
        labels {
        name: "fraudulent"
        variant: "quickstart"
        }
        primaryData {
        table {
            name: "Transactions"
        }
        }
        ```

        Args:
            name (str): Name of source to be retrieved
            variant (str): Name of variant of source

        Returns:
            source (Union[Source, SourceVariant]): Source or SourceVariant
        """
        if local:
            if not variant:
                return get_resource_info_local("source", name)
            return get_source_variant_info_local(name, variant)
        if not variant:
            return get_resource_info(self._stub, "source", name)
        return get_source_variant_info(self._stub, name, variant)

    def list_features(self, local=False):
        """List all features.

        **Examples:**
        ``` py title="Input"
        features_list = rc.list_features()
        ```

        ``` json title="Output"
        // list_features prints out formatted information on all features

        NAME                           VARIANT                        STATUS
        user_age                       quickstart (default)           READY
        avg_transactions               quickstart (default)           READY
        avg_transactions               production                     CREATED
        ```

        ``` py title="Input"
        print(features_list)
        ```

        ``` json title="Output"
        // list_features returns a list of Feature objects

        [name: "user_age"
        default_variant: "quickstart"
        variants: "quickstart"
        , name: "avg_transactions"
        default_variant: "quickstart"
        variants: "quickstart"
        variants: "production"
        ]
        ```

        Returns:
            features (List[Feature]): List of Feature Objects
        """
        if local:
            return list_local("feature", [ColumnName.NAME, ColumnName.VARIANT, ColumnName.STATUS])
        return list_name_variant_status(self._stub, "feature")

    def list_labels(self, local=False):
        """List all labels.

        **Examples:**
        ``` py title="Input"
        features_list = rc.list_labels()
        ```

        ``` json title="Output"
        // list_labels prints out formatted information on all labels

        NAME                           VARIANT                        STATUS
        user_age                       quickstart (default)           READY
        avg_transactions               quickstart (default)           READY
        avg_transactions               production                     CREATED
        ```

        ``` py title="Input"
        print(label_list)
        ```

        ``` json title="Output"
        // list_features returns a list of Feature objects

        [name: "user_age"
        default_variant: "quickstart"
        variants: "quickstart"
        , name: "avg_transactions"
        default_variant: "quickstart"
        variants: "quickstart"
        variants: "production"
        ]
        ```

        Returns:
            labels (List[Label]): List of Label Objects
        """
        if local:
            return list_local("label", [ColumnName.NAME, ColumnName.VARIANT, ColumnName.STATUS])
        return list_name_variant_status(self._stub, "label")

    def list_users(self, local=False):
        """List all users. Prints a list of all users.

        **Examples:**
        ``` py title="Input"
        users_list = rc.list_users()
        ```

        ``` json title="Output"
        // list_users prints out formatted information on all users

        NAME                           STATUS
        featureformer                  NO_STATUS
        featureformers_friend          CREATED
        ```

        ``` py title="Input"
        print(features_list)
        ```

        ``` json title="Output"
        // list_features returns a list of Feature objects

        [name: "featureformer"
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
        },
        name: "featureformers_friend"
        features {
        name: "user_age"
        variant: "production"
        }
        sources {
        name: "user_profiles"
        variant: "production"
        }
        ]
        ```

        Returns:
            users (List[User]): List of User Objects
        """
        if local:
            return list_local("user", [ColumnName.NAME, ColumnName.STATUS])
        return list_name_status(self._stub, "user")

    def list_entities(self, local=False):
        """List all entities. Prints a list of all entities.

        **Examples:**
        ``` py title="Input"
        entities = rc.list_entities()
        ```

        ``` json title="Output"
        // list_entities prints out formatted information on all entities

        NAME                           STATUS
        user                           CREATED
        transaction                    CREATED
        ```

        ``` py title="Input"
        print(features_list)
        ```

        ``` json title="Output"
        // list_entities returns a list of Entity objects

        [name: "user"
        features {
        name: "avg_transactions"
        variant: "quickstart"
        }
        features {
        name: "avg_transactions"
        variant: "production"
        }
        features {
        name: "user_age"
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
        ,
        name: "transaction"
        features {
        name: "amount_spent"
        variant: "production"
        }
        ]
        ```

        Returns:
            entities (List[Entity]): List of Entity Objects
        """
        if local:
            return list_local("entity", [ColumnName.NAME, ColumnName.STATUS])
        return list_name_status(self._stub, "entity")

    def list_sources(self, local=False):
        """List all sources. Prints a list of all sources.

        **Examples:**
        ``` py title="Input"
        sources_list = rc.list_sources()
        ```

        ``` json title="Output"
        // list_sources prints out formatted information on all sources

        NAME                           VARIANT                        STATUS                         DESCRIPTION
        average_user_transaction       quickstart (default)           NO_STATUS                      the average transaction amount for a user
        transactions                   kaggle (default)               NO_STATUS                      Fraud Dataset From Kaggle
        ```

        ``` py title="Input"
        print(sources_list)
        ```

        ``` json title="Output"
        // list_sources returns a list of Source objects

        [name: "average_user_transaction"
        default_variant: "quickstart"
        variants: "quickstart"
        , name: "transactions"
        default_variant: "kaggle"
        variants: "kaggle"
        ]
        ```

        Returns:
            sources (List[Source]): List of Source Objects
        """
        if local:
            return list_local("source",
                              [ColumnName.NAME, ColumnName.VARIANT, ColumnName.STATUS, ColumnName.DESCRIPTION])
        return list_name_variant_status_desc(self._stub, "source")

    def list_training_sets(self, local=False):
        """List all training sets. Prints a list of all training sets.

        **Examples:**
        ``` py title="Input"
        training_sets_list = rc.list_training_sets()
        ```

        ``` json title="Output"
        // list_training_sets prints out formatted information on all training sets

        NAME                           VARIANT                        STATUS                         DESCRIPTION
        fraud_training                 quickstart (default)           READY                          Training set for fraud detection.
        fraud_training                 v2                             CREATED                        Improved training set for fraud detection.
        recommender                    v1 (default)                   CREATED                        Training set for recommender system.
        ```

        ``` py title="Input"
        print(training_sets_list)
        ```

        ``` json title="Output"
        // list_training_sets returns a list of TrainingSet objects

        [name: "fraud_training"
        default_variant: "quickstart"
        variants: "quickstart", "v2",
        name: "recommender"
        default_variant: "v1"
        variants: "v1"
        ]
        ```

        Returns:
            training_sets (List[TrainingSet]): List of TrainingSet Objects
        """
        if local:
            return list_local("training-set", [ColumnName.NAME, ColumnName.VARIANT, ColumnName.STATUS])
        return list_name_variant_status_desc(self._stub, "training-set")

    def list_models(self, local=False):
        """List all models. Prints a list of all models.

        Returns:
            models (List[Model]): List of Model Objects
        """
        if local:
            return list_local("model", [ColumnName.NAME, ColumnName.STATUS, ColumnName.DESCRIPTION])
        return list_name_status_desc(self._stub, "model")

    def list_providers(self, local=False):
        """List all providers. Prints a list of all providers.

        **Examples:**
        ``` py title="Input"
        providers_list = rc.list_providers()
        ```

        ``` json title="Output"
        // list_providers prints out formatted information on all providers

        NAME                           STATUS                         DESCRIPTION
        redis-quickstart               CREATED                      A Redis deployment we created for the Featureform quickstart
        postgres-quickstart            CREATED                      A Postgres deployment we created for the Featureform quickst
        ```

        ``` py title="Input"
        print(providers_list)
        ```

        ``` json title="Output"
        // list_providers returns a list of Providers objects

        [name: "redis-quickstart"
        description: "A Redis deployment we created for the Featureform quickstart"
        type: "REDIS_ONLINE"
        software: "redis"
        serialized_config: "{\"Addr\": \"quickstart-redis:6379\", \"Password\": \"\", \"DB\": 0}"
        features {
        name: "avg_transactions"
        variant: "quickstart"
        }
        features {
        name: "avg_transactions"
        variant: "production"
        }
        features {
        name: "user_age"
        variant: "quickstart"
        }
        , name: "postgres-quickstart"
        description: "A Postgres deployment we created for the Featureform quickstart"
        type: "POSTGRES_OFFLINE"
        software: "postgres"
        serialized_config: "{\"Host\": \"quickstart-postgres\", \"Port\": \"5432\", \"Username\": \"postgres\", \"Password\": \"password\", \"Database\": \"postgres\"}"
        sources {
        name: "transactions"
        variant: "kaggle"
        }
        sources {
        name: "average_user_transaction"
        variant: "quickstart"
        }
        trainingsets {
        name: "fraud_training"
        variant: "quickstart"
        }
        labels {
        name: "fraudulent"
        variant: "quickstart"
        }
        ]
        ```

        Returns:
            providers (List[Provider]): List of Provider Objects
        """
        if local:
            return list_local("provider", [ColumnName.NAME, ColumnName.STATUS, ColumnName.DESCRIPTION])
        return list_name_status_desc(self._stub, "provider")


global_registrar = Registrar()
state = global_registrar.state
clear_state = global_registrar.clear_state
register_user = global_registrar.register_user
register_redis = global_registrar.register_redis
register_blob_store = global_registrar.register_blob_store
register_bigquery = global_registrar.register_bigquery
register_firestore = global_registrar.register_firestore
register_cassandra = global_registrar.register_cassandra
register_dynamodb = global_registrar.register_dynamodb
register_mongodb = global_registrar.register_mongodb
register_snowflake = global_registrar.register_snowflake
register_snowflake_legacy = global_registrar.register_snowflake_legacy
register_postgres = global_registrar.register_postgres
register_redshift = global_registrar.register_redshift
register_spark = global_registrar.register_spark
register_k8s = global_registrar.register_k8s
register_s3 = global_registrar.register_s3
register_local = global_registrar.register_local
register_entity = global_registrar.register_entity
register_column_resources = global_registrar.register_column_resources
register_training_set = global_registrar.register_training_set
sql_transformation = global_registrar.sql_transformation
register_sql_transformation = global_registrar.register_sql_transformation
get_entity = global_registrar.get_entity
get_source = global_registrar.get_source
get_local_provider = global_registrar.get_local_provider
get_redis = global_registrar.get_redis
get_postgres = global_registrar.get_postgres
get_mongodb = global_registrar.get_mongodb
get_snowflake = global_registrar.get_snowflake
get_redshift = global_registrar.get_redshift
get_bigquery = global_registrar.get_bigquery
get_spark = global_registrar.get_spark
get_kubernetes = global_registrar.get_kubernetes
get_blob_store = global_registrar.get_blob_store
get_s3 = global_registrar.get_s3
ResourceStatus = ResourceStatus

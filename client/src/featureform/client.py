#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

import json
from datetime import datetime
from typing import List, Optional, Union

from google.protobuf.timestamp_pb2 import Timestamp
from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table
from typeguard import typechecked

from .constants import NO_RECORD_LIMIT
from .enums import FileFormat, DataResourceType
from .proto import metadata_pb2 as pb
from .register import (
    FeatureColumnResource,
    FileStoreProvider,
    OfflineSQLProvider,
    OfflineSparkProvider,
    OnlineProvider,
    ResourceClient,
    ResourceVariant,
    SourceRegistrar,
    SubscriptableTransformation,
    TrainingSetVariant,
    global_registrar,
    register_user,
)
from .resources import (
    AWSAssumeRoleCredentials,
    AWSStaticCredentials,
    AzureFileStoreConfig,
    BasicCredentials,
    BigQueryConfig,
    CassandraConfig,
    ClickHouseConfig,
    DynamodbConfig,
    FirestoreConfig,
    GCPCredentials,
    GCSFileStoreConfig,
    HDFSConfig,
    MongoDBConfig,
    OnlineBlobConfig,
    PineconeConfig,
    PostgresConfig,
    Provider,
    RedisConfig,
    RedshiftConfig,
    S3StoreConfig,
    SnowflakeCatalog,
    SnowflakeConfig,
    SparkConfig,
    SnowflakeDynamicTableConfig,
)
from .serving import ServingClient


class Client(ResourceClient, ServingClient):
    """
    Client for interacting with Featureform APIs (resources and serving)

    **Using the Client:**
    ```py title="definitions.py"
    import featureform as ff
    from featureform import Client

    client = Client()

    # Example 1: Get a registered provider
    redis = client.get_provider("redis-quickstart")

    # Example 2: Compute a dataframe from a registered source
    transactions_df = client.dataframe("transactions", "quickstart")
    ```
    """

    def __init__(
        self,
        host=None,
        local=False,
        insecure=False,
        cert_path=None,
        dry_run=False,
        debug=False,
    ):
        if local:
            raise Exception(
                "Local mode is not supported in this version. Use featureform <= 1.12.0 for localmode"
            )

        if host is not None:
            self._validate_host(host)

        ResourceClient.__init__(
            self,
            host=host,
            local=local,
            insecure=insecure,
            cert_path=cert_path,
            dry_run=dry_run,
            debug=debug,
        )
        ServingClient.__init__(
            self,
            host=host,
            local=local,
            insecure=insecure,
            cert_path=cert_path,
            debug=debug,
        )

        subject = "default_owner"
        register_user(subject).make_default_owner()

    def dataframe(
        self,
        source: Union[
            SourceRegistrar, SubscriptableTransformation, ResourceVariant, str
        ],
        variant: Optional[str] = None,
        limit=NO_RECORD_LIMIT,
        spark_session=None,
        asynchronous=False,
        verbose=False,
        iceberg=False,
    ):
        """
        Return a dataframe from a registered source or transformation

        **Example:**
        ```py title="definitions.py"
        transactions_df = client.dataframe("transactions", "quickstart")

        avg_user_transaction_df = transactions_df.groupby("CustomerID")["TransactionAmount"].mean()
        ```

        Args:
            source (Union[SourceRegistrar, SubscriptableTransformation, str]): The source or transformation to compute the dataframe from
            variant (str): The source variant; can't be None if source is a string
            limit (int): The maximum number of records to return; defaults to NO_RECORD_LIMIT
            spark_session: Specifices to read as a spark session.
            asynchronous (bool): Flag to determine whether the client should wait for resources to be in either a READY or FAILED state before returning. Defaults to False to ensure that newly registered resources are in a READY state prior to serving them as dataframes.

        Returns:
            df (pandas.DataFrame): The dataframe computed from the source or transformation

        """
        self.apply(asynchronous=asynchronous, verbose=verbose)
        if isinstance(
            source, (SourceRegistrar, SubscriptableTransformation, ResourceVariant)
        ):
            name, variant = source.name_variant()
        elif isinstance(source, str):
            name = source
            if variant is None:
                raise ValueError("variant must be specified if source is a string")
            if variant == "":
                raise ValueError("variant cannot be an empty string")
        else:
            raise ValueError(
                f"source must be of type SourceRegistrar, SubscriptableTransformation or str, not {type(source)}\n"
                "use client.dataframe(name, variant) or client.dataframe(source) or client.dataframe(transformation)"
            )
        if spark_session is not None:
            if isinstance(source, str):
                raise ValueError(
                    "Name/variant strings not supported with spark session. Use object."
                )
            return self._spark_dataframe(source, spark_session)
        if iceberg:
            return self._iceberg_dataframe(source)
        return self.impl._get_source_as_df(name, variant, limit)

    def _get_iceberg_table(
        self,
        source: Union[
            SourceRegistrar, SubscriptableTransformation, ResourceVariant, str
        ],
    ) -> Table:
        catalog = load_catalog(
            "ff_glue",
            **{
                "type": "glue",
                "region_name": "us-east-1",  # pass in glue region
            },
        )
        location = self.location(source)

        return catalog.load_table(location)

    def _iceberg_dataframe(
        self,
        source: Union[
            SourceRegistrar, SubscriptableTransformation, ResourceVariant, str
        ],
    ):
        return self._get_iceberg_table(source).scan().to_pandas()

    # TODO, combine this with the dataset logic in serving.py
    def _spark_dataframe(self, source, spark_session):
        location = self.location(source)

        # If the location is a part-file, we want to get the directory instead.
        is_individual_part_file = location.split("/")[-1].startswith("part-")
        if is_individual_part_file:
            location = "/".join(location.split("/")[:-1])

        # If the schema is s3://, we want to convert it to s3a://.
        location = (
            location.replace("s3://", "s3a://")
            if location.startswith("s3://")
            else location
        )
        file_format = FileFormat.get_format(location, default="parquet")
        if file_format not in FileFormat.supported_formats():
            raise Exception(
                f"file type '{file_format}' is not supported. Please use 'csv' or 'parquet'"
            )

        try:
            df = (
                spark_session.read.option("header", "true")
                .option("recursiveFileLookup", "true")
                .format(file_format)
                .load(location)
            )

            label_column_name = ""
            for col in df.columns:
                if col.startswith("Label__"):
                    label_column_name = col
                    break

            if label_column_name != "":
                df = df.withColumnRenamed(label_column_name, "Label")
        except Exception as e:
            raise Exception(
                f"please make sure the spark session has ability to read '{location}': {e}"
            )

        return df

    def nearest(self, feature, vector, k):
        """
        Query the K nearest neighbors of a provider vector in the index of a registered feature variant

        **Example:**

        ```py title="definitions.py"
        # Get the 5 nearest neighbors of the vector [0.1, 0.2, 0.3] in the index of the feature "my_feature" with variant "my_variant"
        nearest_neighbors = client.nearest("my_feature", "my_variant", [0.1, 0.2, 0.3], 5)
        print(nearest_neighbors) # prints a list of entities (e.g. ["entity1", "entity2", "entity3", "entity4", "entity5"])
        ```

        Args:
            feature (Union[FeatureColumnResource, tuple(str, str)]): Feature object or tuple of Feature name and variant
            vector (List[float]): Query vector
            k (int): Number of nearest neighbors to return

        """
        if isinstance(feature, tuple):
            name, variant = feature
        elif isinstance(feature, FeatureColumnResource):
            name = feature.name
            variant = feature.variant
        else:
            raise Exception(
                f"the feature '{feature}' of type '{type(feature)}' is not support."
                "Feature must be a tuple of (name, variant) or a FeatureColumnResource"
            )

        if k < 1:
            raise ValueError(f"k must be a positive integer")
        return self.impl._nearest(name, variant, vector, k)

    @typechecked
    def write_feature(
        self,
        name_variant: tuple,
        entity: str,
        value: Union[str, int, float, bool, List[float]],
    ):
        name, variant = name_variant

        ts = Timestamp()
        # consider allowing user to pass in timestamp
        ts.GetCurrentTime()

        feature = pb.StreamingFeatureVariant(
            name=name,
            variant=variant,
            entity=entity,
            value=str(value),
            ts=ts,
        )

        features = iter([feature])
        # TODO: add retry logic
        self._stub.WriteFeatures(features)

    @typechecked
    def write_label(
        self,
        name_variant: tuple,
        entity: str,
        value: Union[str, int, float, bool, List[float]],
        timestamp: Optional[datetime] = None,
    ):
        name, variant = name_variant

        ts = Timestamp()
        if timestamp is None:
            ts.GetCurrentTime()
        else:
            ts.FromDatetime(timestamp)

        label = pb.StreamingLabelVariant(
            name=name,
            variant=variant,
            entity=entity,
            value=str(value),
            ts=ts,
        )

        labels = iter([label])
        self._stub.WriteLabels(labels)

    def location(
        self,
        source: Union[SourceRegistrar, SubscriptableTransformation, str],
        variant: Optional[str] = None,
        resource_type: Optional[DataResourceType] = None,
    ):
        """
        Returns the location of a registered resource. For SQL resources, it will return the table name
        and for file resources, it will return the file path.

        **Example:**
        ```py title="definitions.py"
        transaction_location = client.location("transactions", "quickstart", ff.SOURCE)
        ```

        Args:
            source (Union[SourceRegistrar, SubscriptableTransformation, str]): The source or transformation to compute the dataframe from
            variant (str): The source variant; can't be None if source is a string
            resource_type (DataResourceType): The type of resource; can be one of ff.SOURCE, ff.FEATURE, ff.LABEL, or ff.TRAINING_SET
        """
        if isinstance(source, (SourceRegistrar, SubscriptableTransformation)):
            name, variant = source.name_variant()
            resource_type = DataResourceType.TRANSFORMATION
        elif isinstance(source, TrainingSetVariant):
            name = source.name
            variant = source.variant
            resource_type = DataResourceType.TRAINING_SET
        elif isinstance(source, str):
            name = source
            if variant is None:
                raise ValueError("variant must be specified if source is a string")
            if variant == "":
                raise ValueError("variant cannot be an empty string")

            if resource_type is None:
                raise ValueError(
                    "resource_type must be specified if source is a string"
                )

        else:
            raise ValueError(
                f"source must be of type SourceRegistrar, SubscriptableTransformation or str, not {type(source)}\n"
                "use client.dataframe(name, variant) or client.dataframe(source) or client.dataframe(transformation)"
            )

        location = self.impl.location(name, variant, resource_type)
        return location

    def close(self):
        """
        Closes the client, closes channel for hosted mode
        """
        self.impl.close()

    def columns(
        self,
        source: Union[SourceRegistrar, SubscriptableTransformation, str],
        variant: Optional[str] = None,
    ):
        """
        Returns the columns of a registered source or transformation

        **Example:**
        ```py title="definitions.py"
        columns = client.columns("transactions", "quickstart")
        ```

        Args:
            source (Union[SourceRegistrar, SubscriptableTransformation, str]): The source or transformation to get the columns from
            variant (str): The source variant; can't be None if source is a string

        Returns:
            columns (List[str]): The columns of the source or transformation
        """
        if isinstance(source, (SourceRegistrar, SubscriptableTransformation)):
            name, variant = source.name_variant()
        elif isinstance(source, str):
            name = source
            if variant is None:
                raise ValueError("variant must be specified if source is a string")
            if variant == "":
                raise ValueError("variant cannot be an empty string")
        else:
            raise ValueError(
                f"source must be of type SourceRegistrar, SubscriptableTransformation or str, not {type(source)}\n"
                "use client.columns(name, variant) or client.columns(source) or client.columns(transformation)"
            )
        return self.impl._get_source_columns(name, variant)

    @staticmethod
    def __create_provider(name, config, function, description, team, tags, properties):
        return Provider(
            name=name,
            function=function,
            description=description,
            team=team,
            config=config,
            tags=[tag for tag in tags.tag] or [],
            properties={key: value for key, value in properties.property.items()} or {},
        )

    def __get_provider(self, name):
        # Get a single provider
        return next(
            self._stub.GetProviders(
                iter([pb.NameRequest(name=pb.Name(name=name), request_id="")])
            )
        )

    def get_postgres(self, name):
        """Get a Postgres provider. The returned object can be used to register additional resources.

        **Examples**:
        ``` py
        postgres = client.get_postgres("postgres-quickstart")
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

        provider = self.__get_provider(name)
        config = provider.serialized_config
        deserialized_config = json.loads(config.decode("utf-8"))

        postgres_config = PostgresConfig(
            host=deserialized_config["Host"],
            port=deserialized_config["Port"],
            database=deserialized_config["Database"],
            user=deserialized_config["Username"],
            password=deserialized_config["Password"],
            sslmode=deserialized_config["SSLMode"],
        )

        offline_provider = self.__create_provider(
            provider.name,
            postgres_config,
            "OFFLINE",
            provider.description,
            provider.team,
            provider.tags,
            provider.properties,
        )

        return OfflineSQLProvider(global_registrar, offline_provider)

    def get_redshift(self, name):
        """Get a Redshift provider. The returned object can be used to register additional resources.

        **Examples**:
        ``` py
        redshift = client.get_redshift("redshift-quickstart")
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

        provider = self.__get_provider(name)
        config = provider.serialized_config
        deserialized_config = json.loads(config.decode("utf-8"))

        redshift_config = RedshiftConfig(
            host=deserialized_config["Host"],
            port=deserialized_config["Port"],
            database=deserialized_config["Database"],
            user=deserialized_config["Username"],
            password=deserialized_config["Password"],
            sslmode=deserialized_config["SSLMode"],
        )

        offline_provider = self.__create_provider(
            provider.name,
            redshift_config,
            "OFFLINE",
            provider.description,
            provider.team,
            provider.tags,
            provider.properties,
        )

        return OfflineSQLProvider(global_registrar, offline_provider)

    def get_clickhouse(self, name):
        """Get a ClickHouse provider. The returned object can be used to register additional resources.

        **Examples**:
        ``` py
        clickhouse = client.get_clickhouse("clickhouse-quickstart")
        transactions = clickhouse.register_table(
            name="transactions",
            variant="kaggle",
            description="Fraud Dataset From Kaggle",
            table="Transactions",  # This is the table's name in Postgres
        )
        ```

        Args:
            name (str): Name of Clickhouse provider to be retrieved

        Returns:
            clickhouse (OfflineSQLProvider): Provider
        """

        provider = self.__get_provider(name)
        config = provider.serialized_config
        deserialized_config = json.loads(config.decode("utf-8"))

        clickhouse_config = ClickHouseConfig(
            host=deserialized_config["Host"],
            port=deserialized_config["Port"],
            database=deserialized_config["Database"],
            user=deserialized_config["Username"],
            password=deserialized_config["Password"],
            ssl=deserialized_config["SSL"],
        )

        offline_provider = self.__create_provider(
            provider.name,
            clickhouse_config,
            "OFFLINE",
            provider.description,
            provider.team,
            provider.tags,
            provider.properties,
        )

        return OfflineSQLProvider(global_registrar, offline_provider)

    def get_bigquery(self, name):
        """Get a BigQuery provider. The returned object can be used to register additional resources.

        **Examples**:
        ``` py
        bigquery = client.get_bigquery("bigquery-quickstart")
        transactions = bigquery.register_table(
            name="transactions",
            variant="kaggle",
            description="Fraud Dataset From Kaggle",
            project_id="my_project_id",
            dataset_id="my_dataset_id",
            table="Transactions",
        )
        ```
        Args:
            name (str): Name of BigQuery provider to be retrieved
        Returns:
            bigquery (OfflineSQLProvider): Provider
        """

        provider = self.__get_provider(name)
        config = provider.serialized_config
        deserialized_config = json.loads(config.decode("utf-8"))
        bigquery_config = BigQueryConfig(
            project_id=deserialized_config["ProjectID"],
            dataset_id=deserialized_config["DatasetID"],
            credentials=GCPCredentials(
                project_id=deserialized_config["ProjectID"],
                credential_json=deserialized_config["Credentials"],
            ),
        )

        offline_provider = self.__create_provider(
            provider.name,
            bigquery_config,
            "OFFLINE",
            provider.description,
            provider.team,
            provider.tags,
            provider.properties,
        )

        return OfflineSQLProvider(global_registrar, offline_provider)

    def get_cassandra(self, name):
        """Get a Cassandra provider. The returned object can be used to register additional resources.

        **Examples**:
        ``` py
        cassandra = client.get_cassandra("cassandra-quickstart")
        transactions = cassandra.register_table(
            name="transactions",
            variant="kaggle",
            description="Fraud Dataset From Kaggle",
            host="localhost",
            port="9042",
            username="cassandra",
            password="cassandra",
            keyspace="keyspace",
            consistency="ONE",
            replication="{'class': 'SimpleStrategy', 'replication_factor': 1}"
        )
        ```
        Args:
            name (str): Name of Cassandra provider to be retrieved
        Returns:
            cassandra (OfflineSQLProvider): Provider
        """

        provider = self.__get_provider(name)
        config = provider.serialized_config
        deserialized_config = json.loads(config.decode("utf-8"))

        host, port = self.__split_address(deserialized_config["Addr"])

        cassandra_config = CassandraConfig(
            keyspace=deserialized_config["Keyspace"],
            host=host,
            port=port,
            username=deserialized_config["Username"],
            password=deserialized_config["Password"],
            consistency=deserialized_config["Consistency"],
            replication=deserialized_config["Replication"],
        )

        online_provider = self.__create_provider(
            provider.name,
            cassandra_config,
            "ONLINE",
            provider.description,
            provider.team,
            provider.tags,
            provider.properties,
        )

        return OnlineProvider(global_registrar, online_provider)

    def get_firestore(self, name):
        """Get a Firestore provider. The returned object can be used to register additional resources.

        **Examples**:
        ``` py
        firestore = client.get_firestore("firestore-quickstart")
        transactions = firestore.register_table(
            name="transactions",
            variant="kaggle",
            description="Fraud Dataset From Kaggle",
            project_id="my_project_id",
            collection_id="my_collection_id",
        )
        ```
        Args:
            name (str): Name of Firestore provider to be retrieved
        Returns:
            firestore (OnlineProvider): Provider
        """

        provider = self.__get_provider(name)
        config = provider.serialized_config
        deserialized_config = json.loads(config.decode("utf-8"))

        firestore_config = FirestoreConfig(
            project_id=deserialized_config["ProjectID"],
            collection=deserialized_config["Collection"],
            credentials=GCPCredentials(
                project_id=deserialized_config["ProjectID"],
                credential_json=deserialized_config["Credentials"],
            ),
        )

        online_provider = self.__create_provider(
            provider.name,
            firestore_config,
            "ONLINE",
            provider.description,
            provider.team,
            provider.tags,
            provider.properties,
        )

        return OnlineProvider(global_registrar, online_provider)

    def get_dynamodb(self, name):
        """Get a DynamoDB provider. The returned object can be used to register additional resources.

        **Examples**:
        ``` py
        dynamodb = client.get_dynamodb("dynamodb-quickstart")
        ```
        Args:
            name (str): Name of DynamoDB provider to be retrieved
        Returns:
            dynamodb (OnlineProvider): Provider
        """

        provider = self.__get_provider(name)
        config = provider.serialized_config
        deserialized_config = json.loads(config.decode("utf-8"))

        if deserialized_config["Credentials"]["Type"] == AWSStaticCredentials.type():
            credentials = AWSStaticCredentials(
                deserialized_config["Credentials"]["AccessKeyId"],
                deserialized_config["Credentials"]["SecretKey"],
            )
        elif (
            deserialized_config["Credentials"]["Type"]
            == AWSAssumeRoleCredentials.type()
        ):
            credentials = AWSAssumeRoleCredentials()
        else:
            raise ValueError("Invalid Credentials Type")

        dynamodb_config = DynamodbConfig(
            region=deserialized_config["Region"],
            credentials=credentials,
            should_import_from_s3=deserialized_config["ImportFromS3"],
        )

        online_provider = self.__create_provider(
            provider.name,
            dynamodb_config,
            "ONLINE",
            provider.description,
            provider.team,
            provider.tags,
            provider.properties,
        )

        return OnlineProvider(global_registrar, online_provider)

    # We combine the redis host and port into an address in the config. Need to split it to recreate the
    # host and port variables
    @staticmethod
    def __split_address(addr):
        address = addr
        split_address = address.split(":")

        # If the host has `:` in it then rejoin the string
        host = "".join(split_address[:-1])

        # The Port should always be a number at the very end
        port = split_address[-1]
        return host, int(port)

    def get_redis(self, name):
        """Get a Redis provider. The returned object can be used to register additional resources.

        **Examples**:
        ``` py
        redis = client.get_redis("redis-quickstart")
        ```
        Args:
            name (str): Name of Redis provider to be retrieved
        Returns:
            redis (OnlineProvider): Provider
        """

        provider = self.__get_provider(name)
        config = provider.serialized_config
        deserialized_config = json.loads(config.decode("utf-8"))

        host, port = self.__split_address(deserialized_config["Addr"])

        redis_config = RedisConfig(
            host=host,
            port=port,
            password=deserialized_config["Password"],
            db=deserialized_config["DB"],
        )

        online_provider = self.__create_provider(
            provider.name,
            redis_config,
            "ONLINE",
            provider.description,
            provider.team,
            provider.tags,
            provider.properties,
        )

        return OnlineProvider(global_registrar, online_provider)

    def get_snowflake(self, name):
        """Get a Snowflake provider. The returned object can be used to register additional resources.

        **Examples**:
        ``` py
        snowflake = client.get_snowflake("snowflake-quickstart")
        transactions = snowflake.register_table(
            name="transactions",
            variant="kaggle",
            description="Fraud Dataset From Kaggle",
            account="my_account",
            warehouse="my_warehouse",
            database="my_database",
            schema="my_schema",
            table="Transactions"
        )
        ```
        Args:
            name (str): Name of Snowflake provider to be retrieved
        Returns:
            snowflake (OfflineSQLProvider): Provider
        """

        provider = self.__get_provider(name)
        config = provider.serialized_config
        deserialized_config = json.loads(config.decode("utf-8"))

        catalog = None
        if deserialized_config["Catalog"]:
            catalog = SnowflakeCatalog(
                external_volume=deserialized_config["Catalog"]["ExternalVolume"],
                base_location=deserialized_config["Catalog"]["BaseLocation"],
                table_config=SnowflakeDynamicTableConfig(
                    target_lag=deserialized_config["Catalog"]["TableConfig"][
                        "TargetLag"
                    ],
                    # While RefreshMode and Initialize are stored as protos in SQLTransformation,
                    # provider configs are stored as serialized JSON strings; given this, we set
                    # the string representation here given this is the value the backend actually
                    # expects and uses.
                    refresh_mode=RefreshMode.from_string(
                        deserialized_config["Catalog"]["TableConfig"]["RefreshMode"]
                    ),
                    initialize=Initialize.from_string(
                        deserialized_config["Catalog"]["TableConfig"]["Initialize"]
                    ),
                ),
            )

        snowflake_config = SnowflakeConfig(
            account=deserialized_config["Account"],
            warehouse=deserialized_config["Warehouse"],
            database=deserialized_config["Database"],
            schema=deserialized_config["Schema"],
            username=deserialized_config["Username"],
            password=deserialized_config["Password"],
            role=deserialized_config["Role"],
            organization=deserialized_config["Organization"],
            catalog=catalog,
        )

        offline_provider = self.__create_provider(
            provider.name,
            snowflake_config,
            "OFFLINE",
            provider.description,
            provider.team,
            provider.tags,
            provider.properties,
        )

        return OfflineSQLProvider(global_registrar, offline_provider)

    def get_mongodb(self, name):
        """Get a MongoDB provider. The returned object can be used to register additional resources.

        **Examples**:
        ``` py
        mongodb = client.get_mongodb("mongodb-quickstart")
        ```
        Args:
            name (str): Name of MongoDB provider to be retrieved
        Returns:
            mongodb (OnlineProvider): Provider
        """

        provider = self.__get_provider(name)
        config = provider.serialized_config
        deserialized_config = json.loads(config.decode("utf-8"))

        mongodb_config = MongoDBConfig(
            username=deserialized_config["Username"],
            password=deserialized_config["Password"],
            database=deserialized_config["Database"],
            host=deserialized_config["Host"],
            port=deserialized_config["Port"],
            throughput=deserialized_config["Throughput"],
        )

        online_provider = self.__create_provider(
            provider.name,
            mongodb_config,
            "ONLINE",
            provider.description,
            provider.team,
            provider.tags,
            provider.properties,
        )

        return OnlineProvider(global_registrar, online_provider)

    def get_pinecone(self, name):
        """Get a Pinecone provider. The returned object can be used to register additional resources.

        **Examples**:
        ``` py
        pinecone = client.get_pinecone("pinecone-quickstart")
        ```
        Args:
            name (str): Name of Pinecone provider to be retrieved
        Returns:
            pinecone (OnlineProvider): Provider
        """

        provider = self.__get_provider(name)
        config = provider.serialized_config
        deserialized_config = json.loads(config.decode("utf-8"))

        pinecone_config = PineconeConfig(
            api_key=deserialized_config["ApiKey"],
            project_id=deserialized_config["ProjectID"],
            environment=deserialized_config["Environment"],
        )

        online_provider = self.__create_provider(
            provider.name,
            pinecone_config,
            "ONLINE",
            provider.description,
            provider.team,
            provider.tags,
            provider.properties,
        )

        return OnlineProvider(global_registrar, online_provider)

    def get_snowflake_legacy(self, name):
        """Get a legacy Snowflake provider. The returned object can be used to register additional resources.

        **Examples**:
        ``` py
        snowflake = client.get_snowflake_legacy("snowflake-quickstart")
        transactions = snowflake.register_table(
            name="transactions",
            variant="kaggle",
            description="Fraud Dataset From Kaggle",
            account="my_account",
            warehouse="my_warehouse",
            database="my_database",
            schema="my_schema",
            table="Transactions"
        )
        ```
        Args:
            name (str): Name of Snowflake provider to be retrieved
        Returns:
            snowflake (OfflineSQLProvider): Provider
        """

        provider = self.__get_provider(name)
        config = provider.serialized_config
        deserialized_config = json.loads(config.decode("utf-8"))

        legacy_snowflake_config = SnowflakeConfig(
            account_locator=deserialized_config["AccountLocator"],
            warehouse=deserialized_config["Warehouse"],
            database=deserialized_config["Database"],
            schema=deserialized_config["Schema"],
            username=deserialized_config["Username"],
            password=deserialized_config["Password"],
            role=deserialized_config["Role"],
        )

        offline_provider = self.__create_provider(
            provider.name,
            legacy_snowflake_config,
            "OFFLINE",
            provider.description,
            provider.team,
            provider.tags,
            provider.properties,
        )

        return OfflineSQLProvider(global_registrar, offline_provider)

    def get_s3(self, name):
        """
        Get a S3 provider. The returned object can be used with other providers such as Spark and Databricks.

        **Examples**:

        ``` py

        s3 = client.get_s3("s3-quickstart")
        spark = ff.register_spark(
            name=f"spark-emr-s3",
            description="A Spark deployment we created for the Featureform quickstart",
            team="featureform-team",
            executor=emr,
            filestore=s3,
        )
        ```

        Args:
            name (str): Name of S3 to be retrieved

        Returns:
            s3 (FileStore): Provider
        """
        provider = self.__get_provider(name)
        config = provider.serialized_config
        deserialized_config = json.loads(config.decode("utf-8"))

        if deserialized_config["Credentials"]["Type"] == AWSStaticCredentials.type():
            credentials = AWSStaticCredentials(
                deserialized_config["Credentials"]["AccessKeyId"],
                deserialized_config["Credentials"]["SecretKey"],
            )
        elif (
            deserialized_config["Credentials"]["Type"]
            == AWSAssumeRoleCredentials.type()
        ):
            credentials = AWSAssumeRoleCredentials()
        else:
            raise ValueError("Invalid Credentials Type")

        s3_config = S3StoreConfig(
            bucket_path=deserialized_config["BucketPath"],
            bucket_region=deserialized_config["BucketRegion"],
            path=deserialized_config["Path"],
            credentials=credentials,
        )

        provider = self.__create_provider(
            provider.name,
            s3_config,
            "OFFLINE",
            provider.description,
            provider.team,
            provider.tags,
            provider.properties,
        )

        return FileStoreProvider(
            registrar=global_registrar,
            provider=provider,
            config=s3_config,
            store_type=s3_config.type(),
        )

    def get_spark(self, name):
        """
        Get a Spark provider. The returned object can be used to register additional resources.

        **Examples**:

        ``` py

        spark = client.get_spark("spark-quickstart")
        transactions = spark.register_table(
            name="transactions",
            variant="kaggle",
            description="Fraud Dataset From Kaggle",
            table="Transactions",
        )
        ```

        Args:
            name (str): Name of Spark provider to be retrieved

        Returns:
            spark (OfflineSQLProvider): Provider
        """
        provider = self.__get_provider(name)
        config_bytes = provider.serialized_config
        spark_config = SparkConfig.deserialize(config_bytes)

        offline_provider = self.__create_provider(
            provider.name,
            spark_config,
            "OFFLINE",
            provider.description,
            provider.team,
            provider.tags,
            provider.properties,
        )

        return OfflineSparkProvider(global_registrar, offline_provider)

    def get_blob_store(self, name):
        """
        Get a Blob Store provider. The returned object can be used to register additional resources.

        **Examples**:

        ``` py

        blob_store = client.get_blob_store("blob-store-quickstart")
        ```

        Args:
            name (str): Name of Blob Store provider to be retrieved

        Returns:
            blob_store (FileStoreProvider): Provider
        """
        provider = self.__get_provider(name)
        config = provider.serialized_config
        deserialized_config = json.loads(config.decode("utf-8"))

        azure_config = AzureFileStoreConfig(
            account_name=deserialized_config["AccountName"],
            account_key=deserialized_config["AccountKey"],
            container_name=deserialized_config["ContainerName"],
            root_path=deserialized_config["Path"],
        )

        blob_config = OnlineBlobConfig(
            store_type="AZURE", store_config=azure_config.config()
        )

        provider = self.__create_provider(
            provider.name,
            blob_config,
            "ONLINE",
            provider.description,
            provider.team,
            provider.tags,
            provider.properties,
        )

        return FileStoreProvider(
            registrar=global_registrar,
            provider=provider,
            config=azure_config,
            store_type="AZURE",
        )

    def get_gcs(self, name):
        """
        Get a GCS provider. The returned object can be used to register additional resources.

        **Examples**:

        ``` py

        gcs = client.get_gcs("gcs-quickstart")
        ```

        Args:
            name (str): Name of GCS provider to be retrieved

        Returns:
            gcs (FileStoreProvider): Provider
        """
        provider = self.__get_provider(name)
        config = provider.serialized_config
        deserialized_config = json.loads(config.decode("utf-8"))

        gcs_config = GCSFileStoreConfig(
            bucket_name=deserialized_config["BucketName"],
            bucket_path=deserialized_config["BucketPath"],
            credentials=GCPCredentials(
                project_id=deserialized_config["Credentials"]["ProjectId"],
                credential_json=deserialized_config["Credentials"]["JSON"],
            ),
        )

        provider = self.__create_provider(
            provider.name,
            gcs_config,
            "OFFLINE",
            provider.description,
            provider.team,
            provider.tags,
            provider.properties,
        )

        return FileStoreProvider(
            registrar=global_registrar,
            provider=provider,
            config=gcs_config,
            store_type=gcs_config.type(),
        )

    def get_hdfs(self, name):
        provider = self.__get_provider(name)
        config = provider.serialized_config
        deserialized_config = json.loads(config.decode("utf-8"))

        hdfs_config = HDFSConfig(
            host=deserialized_config["Host"],
            port=deserialized_config["Port"],
            path=deserialized_config["Path"],
            credentials=BasicCredentials(
                username=deserialized_config["CredentialConfig"]["Username"],
                password=deserialized_config["CredentialConfig"]["Password"],
            ),
            hdfs_site_contents=deserialized_config["HDFSSiteConf"],
            core_site_contents=deserialized_config["CoreSiteConf"],
        )

        provider = self.__create_provider(
            provider.name,
            hdfs_config,
            "OFFLINE",
            provider.description,
            provider.team,
            provider.tags,
            provider.properties,
        )

        return FileStoreProvider(
            registrar=global_registrar,
            provider=provider,
            config=hdfs_config,
            store_type=hdfs_config.type(),
        )

    @staticmethod
    def _validate_host(host):
        if host.startswith("http://") or host.startswith("https://"):
            raise ValueError("Invalid Host: Host should not contain http or https.")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

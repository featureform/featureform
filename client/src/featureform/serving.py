# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

import base64
import inspect
import json
import math
import os
import random
import types
import warnings
from typing import List, Union, Dict

import dill
import numpy as np
import pandas as pd
from featureform import metadata
from featureform.proto import serving_pb2
from featureform.proto import serving_pb2_grpc
from featureform.providers import get_provider, Scalar, VectorType
from pandas.core.generic import NDFrame
from pandasql import sqldf

from . import progress_bar
from .register import FeatureColumnResource

from .constants import NO_RECORD_LIMIT
from .enums import FileFormat, ScalarType
from .file_utils import absolute_file_paths
from .local_cache import LocalCache
from .local_utils import (
    get_sql_transformation_sources,
    feature_df_with_entity,
    feature_df_from_csv,
    label_df_from_csv,
    merge_feature_into_ts,
)
from .resources import Model, SourceType, ComputationMode
from .sqlite_metadata import SQLiteMetadata
from .tls import insecure_channel, secure_channel
from .version import check_up_to_date


def check_feature_type(features):
    checked_features = []
    for feature in features:
        if isinstance(feature, tuple):
            checked_features.append(feature)
        elif isinstance(feature, str):
            # TODO: Need to identify how to pull the run id
            checked_features.append((feature, "default"))
        elif isinstance(feature, FeatureColumnResource):
            checked_features.append(feature.name_variant())
        else:
            raise ValueError(
                f"Invalid feature type {type(feature)}; must be a tuple, string, or FeatureColumnResource"
            )
    return checked_features


class ServingClient:
    """
    The serving client is used to retrieve training sets and features for training and serving purposes.
    **Using the Serving Client:**
    ``` py
    import featureform as ff
    from featureform import Client
    client = Client()
    # example:
    dataset = client.training_set("fraud_training", "quickstart")
    training_dataset = dataset.repeat(10).shuffle(1000).batch(8)
    for feature_batch in training_dataset:
        # Train model
    ```
    """

    def __init__(self, host=None, local=False, insecure=False, cert_path=None):
        # This line ensures that the warning is only raised if ServingClient is instantiated directly
        # TODO: Remove this check once ServingClient is deprecated
        is_instantiated_directed = inspect.stack()[1].function != "__init__"
        if is_instantiated_directed:
            warnings.warn(
                "ServingClient is deprecated and will be removed in future versions; use Client instead.",
                PendingDeprecationWarning,
            )
        """
        Args:
            host (str): The hostname of the Featureform instance. Exclude if using Localmode.
            local (bool): True if using Localmode.
            insecure (bool): True if connecting to an insecure Featureform endpoint. False if using a self-signed or public TLS certificate
            cert_path (str): The path to a public certificate if using a self-signed certificate.
        """
        if local and host:
            raise ValueError("Host and local cannot both be set")
        if local:
            self.impl = LocalClientImpl()
        else:
            self.impl = HostedClientImpl(host, insecure, cert_path)

    def training_set(
        self,
        name,
        variant="",
        include_label_timestamp=False,
        model: Union[str, Model] = None,
    ):
        """Return an iterator that iterates through the specified training set.

        **Examples**:
        ``` py
            client = ff.Client()
            dataset = client.training_set("fraud_training", "quickstart")
            training_dataset = dataset.repeat(10).shuffle(1000).batch(8)
            for feature_batch in training_dataset:
                # Train model
        ```

        Args:
            name (str): Name of training set to be retrieved
            variant (str): Variant of training set to be retrieved

        Returns:
            training_set (Dataset): A training set iterator
        """
        return self.impl.training_set(name, variant, include_label_timestamp, model)

    def features(
        self, features, entities, model: Union[str, Model] = None, params: list = None
    ):
        """Returns the feature values for the specified entities.

        **Examples**:
        ``` py
            client = ff.Client()
            fpf = client.features([("avg_transactions", "quickstart")], {"user": "C1410926"})
            # Run features through model
        ```

        Args:
            features (list[(str, str)], list[str]): List of Name Variant Tuples
            entities (dict): Dictionary of entity name/value pairs

        Returns:
            features (numpy.Array): An Numpy array of feature values in the order given by the inputs
        """
        features = check_feature_type(features)
        return self.impl.features(features, entities, model, params)

    def close(self):
        """Closes the connection to the Featureform instance."""
        self.impl.close()

    def batch_features(self, *features):
        """
        Return an iterator that iterates over each entity and corresponding features in feats.
        **Example:**
        ```py title="definitions.py"
        for feature_values in client.batch_features("feature1", "feature2", "feature3"):
            print(feature_values)
        ```

        Args:
            *feats (str): The features to iterate over

        Returns:
            iterator: An iterator of entity and feature values

        """
        if len(features) == 0:
            raise ValueError("No features provided")
        feature_tuples = check_feature_type(features)
        return self.impl.batch_features(feature_tuples)


class HostedClientImpl:
    def __init__(self, host=None, insecure=False, cert_path=None):
        host = host or os.getenv("FEATUREFORM_HOST")
        if host is None:
            raise ValueError(
                "If not in local mode then `host` must be passed or the environment"
                " variable FEATUREFORM_HOST must be set."
            )
        check_up_to_date(False, "serving")
        self._channel = self._create_channel(host, insecure, cert_path)
        self._stub = serving_pb2_grpc.FeatureStub(self._channel)

    def _create_channel(self, host, insecure, cert_path):
        if insecure:
            return insecure_channel(host)
        else:
            return secure_channel(host, cert_path)

    def training_set(
        self, name, variation, include_label_timestamp, model: Union[str, Model] = None
    ):
        return Dataset(self._stub).from_stub(name, variation, model)

    def features(
        self, features, entities, model: Union[str, Model] = None, params: list = None
    ):
        req = serving_pb2.FeatureServeRequest()
        for name, values in entities.items():
            entity_proto = req.entities.add()
            entity_proto.name = name
            if isinstance(values, list):
                for value in values:  # Assuming 'values' is a list of strings
                    entity_proto.values.append(value)
            elif isinstance(values, str):
                entity_proto.values.append(values)
            else:
                raise ValueError(
                    "Entity values must be either a string or a list of strings"
                )
        for name, variation in features:
            feature_id = req.features.add()
            feature_id.name = name
            feature_id.version = variation
        if model is not None:
            req.model.name = model if isinstance(model, str) else model.name
        resp = self._stub.FeatureServe(req)

        deprecated_values = resp.values
        value_lists = (
            deprecated_values if len(deprecated_values) > 0 else resp.value_lists
        )

        feature_values = []
        for val_list in value_lists:
            entity_values = []
            for val in val_list.values:
                parsed_value = parse_proto_value(val)
                value_type = type(parsed_value)

                # Ondemand features are returned as a byte array
                # which holds the pickled function
                if value_type == bytes:
                    code = dill.loads(bytearray(parsed_value))
                    func = types.FunctionType(code, globals(), "transformation")
                    parsed_value = func(self, params, entities)
                # Vector features are returned as a Vector32 proto due
                # to the inability to use the `repeated` keyword in
                # in a `oneof` field
                elif value_type == serving_pb2.Vector32:
                    parsed_value = parsed_value.value
                entity_values.append(parsed_value)

            # If theres only one entity row, only return that row
            if len(value_lists) == 1:
                return entity_values
            feature_values.append(entity_values)

        return feature_values

    def batch_features(self, features):
        return FeatureSetIterator(self._stub, features)

    def _get_source_as_df(self, name, variant, limit):
        columns = self._get_source_columns(name, variant)
        data = self._get_source_data(name, variant, limit)
        return pd.DataFrame(data=data, columns=columns)

    def _get_source_data(self, name, variant, limit):
        id = serving_pb2.SourceID(name=name, version=variant)
        req = serving_pb2.SourceDataRequest(id=id, limit=limit)
        resp = self._stub.SourceData(req)
        data = []
        for rows in resp:
            row = [getattr(r, r.WhichOneof("value")) for r in rows.rows]
            data.append(row)
        return data

    def _get_source_columns(self, name, variant):
        id = serving_pb2.SourceID(name=name, version=variant)
        req = serving_pb2.SourceDataRequest(id=id)
        resp = self._stub.SourceColumns(req)
        # The Python type of resp.columns is <class 'google._upb._message.RepeatedScalarContainer'>
        # which is not a "recognized" type by pandas internal type check, which resulted in the following error:
        # `Index(...) must be called with a collection of some kind`
        # To avoid this issue, we convert the resp.columns to a Python list
        return list(resp.columns)

    def nearest(self, name, variant, vector, k):
        id = serving_pb2.FeatureID(name=name, version=variant)
        vec = serving_pb2.Vector32(value=vector)
        req = serving_pb2.NearestRequest(id=id, vector=vec, k=k)
        resp = self._stub.Nearest(req)
        return resp.entities

    def close(self):
        self._channel.close()


class LocalClientImpl:
    def __init__(self):
        self.db = SQLiteMetadata()
        self.local_cache = LocalCache()
        check_up_to_date(True, "serving")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.db.close()

    def get_training_set_dataframe(
        self, label, label_df, training_set_name, training_set_variant
    ) -> NDFrame:
        def get() -> pd.DataFrame:
            feature_columns = []

            # We will build the training set DF by merging each feature one by one into it.
            training_set_df = label_df
            features = self.db.get_training_set_features(
                training_set_name, training_set_variant
            )
            for feature in features:
                feature_variant = self.db.get_feature_variant(
                    feature["feature_name"], feature["feature_variant"]
                )
                feature_df = self.get_feature_dataframe(feature_variant)
                training_set_df = merge_feature_into_ts(
                    feature_variant, label, feature_df, training_set_df
                )
                feature_columns.append(
                    f"{feature['feature_name']}.{feature['feature_variant']}"
                )

            lag_features = self.db.get_training_set_lag_features(
                training_set_name, training_set_variant
            )
            if len(lag_features) > 0:
                timestamp_column = label["source_timestamp"]
                entity_column = label["source_entity"]
                label_column = "label"
                lag_sql_query = self.get_lag_features_sql_query(
                    lag_features,
                    feature_columns,
                    entity_column,
                    label_column,
                    timestamp_column,
                )

                globals()["source_0"] = training_set_df
                mysql = lambda q: sqldf(q, globals())
                training_set_df = mysql(lag_sql_query)

            return training_set_df

        return self.local_cache.get_or_put_training_set(
            training_set_name=training_set_name,
            training_set_variant=training_set_variant,
            func=get,
        )

    def training_set(
        self,
        training_set_name,
        training_set_variant,
        include_label_timestamp,
        model: Union[str, Model] = None,
    ):
        training_set = self.db.get_training_set_variant(
            training_set_name, training_set_variant
        )

        label = self.db.get_label_variant(
            training_set["label_name"], training_set["label_variant"]
        )
        label_df = self.get_label_dataframe(label)

        if model is not None:
            self._register_model(
                model,
                look_up_table="model_training_sets",
                association_name=training_set_name,
                association_variant=training_set_variant,
            )

        training_set_df = self.get_training_set_dataframe(
            label, label_df, training_set_name, training_set_variant
        )

        return self.convert_ts_df_to_dataset(
            label, training_set_df, include_label_timestamp
        )

    def batch_features(self):
        raise NotImplementedError("batch_features is not supported in local mode")

    def get_lag_features_sql_query(
        self, lag_features, feature_columns, entity, label, ts
    ):
        """
        Returns the SQL query to compute the lag features.
        Input:
        - lag_features: dict

        Returns:
        - sql_query: string
        """

        if len(lag_features) == 0:
            return "SELECT * FROM source_0"

        features = ""
        for f in feature_columns:
            features = f'{features}, "{f}"' if features != "" else f'"{f}"'

        MAIN_SELECT = f"SELECT {entity}, {ts}, {features}"
        SUBQUERY = "FROM (SELECT * FROM (SELECT *, row_number FROM ("
        INNER_QUERY = "FROM (( SELECT * FROM source_0 )"
        CLOSING_QUERY = f") tt ) WHERE row_number=1 ORDER BY {ts} ASC ))"

        lag_columns = []
        lag_timestamps = []

        LAG_QUERIES = ""
        for i, lag_feature in enumerate(lag_features):
            feature_column_name = (
                f"{lag_feature['feature_name']}.{lag_feature['feature_variant']}"
            )
            lag_feature_column_name = lag_feature["feature_new_name"]
            lag = lag_feature["feature_lag"]

            lag_query = f"""
                         LEFT OUTER JOIN ( SELECT * FROM ( SELECT {entity} AS t{i}_entity, \"{feature_column_name}\" AS \"{lag_feature_column_name}\", {ts} AS t{i}_ts
                         FROM source_0) 
                         ORDER BY t{i}_ts ASC) t{i}
                         ON (t{i}_entity = {entity} AND DATETIME(t{i}_ts, \"+{lag} seconds\") <= {ts})
                        """

            LAG_QUERIES = (
                f"{LAG_QUERIES} {lag_query}" if LAG_QUERIES != "" else lag_query
            )
            MAIN_SELECT = f'{MAIN_SELECT}, "{lag_feature_column_name}"'

            lag_columns.append(f'"{lag_feature_column_name}"')
            lag_timestamps.append(f"t{i}_ts")

        lag_columns = ", ".join(lag_columns)
        lag_timestamps = " DESC, ".join(lag_timestamps)

        INNER_SELECT = f"SELECT {entity}, {ts}, {label}, {features}, {lag_columns}, ROW_NUMBER() over (PARTITION BY {entity}, {label}, {ts} ORDER BY {ts} DESC, {lag_timestamps} DESC) as row_number"
        FULL_QUERY = f"{MAIN_SELECT}, {label} {SUBQUERY} {INNER_SELECT} {INNER_QUERY} {LAG_QUERIES} {CLOSING_QUERY}"

        return FULL_QUERY

    def get_input_df(self, source_name, source_variant):
        if (
            self.db.is_transformation(source_name, source_variant)
            == SourceType.PRIMARY_SOURCE
        ):
            source = self.db.get_source_variant(source_name, source_variant)
            file_path = source["definition"]
            if FileFormat.get_format(file_path) == FileFormat.CSV:
                df = pd.read_csv(file_path)
            elif FileFormat.get_format(file_path) == FileFormat.PARQUET:
                df = pd.read_parquet(file_path)
            else:
                raise ValueError(f"Unsupported file format for {file_path}")
            return df
        elif (
            self.db.is_transformation(source_name, source_variant)
            == SourceType.DIRECTORY
        ):
            source = self.db.get_source_variant(source_name, source_variant)
            directory = source["definition"]
            return self.read_directory(directory)

        else:
            df = self.process_transformation(source_name, source_variant)
        return df

    def read_directory(self, directory):
        if not os.path.isdir(directory):
            raise Exception(f"Path {directory} is not a directory")

        file_names = []
        file_body = []
        for absolute_fn, relative_fn in absolute_file_paths(directory):
            file_names.append(relative_fn)
            with open(absolute_fn, "r") as f:
                try:
                    file_body.append(f.read())
                except Exception as e:
                    raise Exception(
                        f"Cannot read file {absolute_fn}: {e}\nFiles must be text files"
                    )
        df = pd.DataFrame(data={"filename": file_names, "body": file_body})
        return df

    def sql_transformation(self, query):
        transformation_sources = get_sql_transformation_sources(query)
        for i, (source_name, source_variant) in enumerate(transformation_sources):
            # Creates a variable called dataframes_i which stores the corresponding df for each input
            df_variable = f"dataframes_{i}"
            # globals()[df_variable]:
            # 1. Converts a string "dataframes_i" to a variable name
            # 2. Assigns a global scope to the variable, to access it outside the loop
            globals()[df_variable] = self.get_input_df(source_name, source_variant)

            # using '+' signs for readability
            query_source_to_replace = "{{" + f"{source_name}.{source_variant}" + "}}"
            query = query.replace(query_source_to_replace, df_variable)

        return sqldf(query, globals())

    def process_transformation(self, name, variant):
        def get():
            source = self.db.get_source_variant(name, variant)
            if (
                self.db.is_transformation(name, variant)
                == SourceType.SQL_TRANSFORMATION.value
            ):
                query = source["definition"]
                new_data = self.sql_transformation(query)
            else:
                code = dill.loads(bytearray(source["definition"]))
                inputs = json.loads(source["inputs"])
                dataframes = []
                for input in inputs:
                    source_name, source_variant = (
                        input[0],
                        input[1],
                    )
                    dataframes.append(self.get_input_df(source_name, source_variant))
                func = types.FunctionType(code, globals(), "transformation")
                new_data = func(*dataframes)
                if new_data is None:
                    raise ValueError(
                        f"Transformation {name} ({variant}) returned None. Please return a dataframe."
                    )
            return new_data

        return self.local_cache.get_or_put(
            resource_type="transformation",
            resource_name=name,
            resource_variant=variant,
            source_name=name,
            source_variant=variant,
            func=get,
        )

    def get_label_dataframe(self, label) -> NDFrame:
        def get() -> pd.DataFrame:
            transform_type = self.db.is_transformation(
                label["source_name"], label["source_variant"]
            )
            if (
                transform_type == SourceType.SQL_TRANSFORMATION.value
                or transform_type == SourceType.DF_TRANSFORMATION.value
            ):
                label_df = self.label_df_from_transformation(label)
            else:
                label_source = self.db.get_source_variant(
                    label["source_name"], label["source_variant"]
                )
                label_df = label_df_from_csv(label, label_source["definition"])
            label_df.rename(columns={label["source_value"]: "label"}, inplace=True)
            return label_df

        try:
            return self.local_cache.get_or_put(
                resource_type="label",
                resource_name=label["name"],
                resource_variant=label["variant"],
                source_name=label["source_name"],
                source_variant=label["source_variant"],
                func=get,
            )
        except Exception as e:
            raise ValueError(
                f"Could not get source for label {label['name']} ({label['variant']}): {e}"
            )

    def label_df_from_transformation(self, label):
        df = self.process_transformation(label["source_name"], label["source_variant"])
        if label["source_timestamp"] != "":
            df = df[
                [
                    label["source_entity"],
                    label["source_value"],
                    label["source_timestamp"],
                ]
            ]
            df[label["source_timestamp"]] = pd.to_datetime(
                df[label["source_timestamp"]]
            )
        else:
            df = df[[label["source_entity"], label["source_value"]]]
        df.set_index(label["source_entity"])
        return df

    def get_feature_dataframe(self, feature) -> NDFrame:
        def get() -> pd.DataFrame:
            name_variant = feature["name"] + "." + feature["variant"]
            transform_type = self.db.is_transformation(
                feature["source_name"], feature["source_variant"]
            )
            if (
                transform_type == SourceType.SQL_TRANSFORMATION.value
                or transform_type == SourceType.DF_TRANSFORMATION.value
            ):
                feature_df = self.feature_df_from_transformation(feature)
            else:
                source = self.db.get_source_variant(
                    feature["source_name"], feature["source_variant"]
                )
                feature_df = feature_df_from_csv(feature, source["definition"])
            feature_df.set_index(feature["source_entity"])
            feature_df.rename(
                columns={feature["source_value"]: name_variant}, inplace=True
            )
            return feature_df

        return self.local_cache.get_or_put(
            resource_type="feature",
            resource_name=feature["name"],
            resource_variant=feature["variant"],
            source_name=feature["source_name"],
            source_variant=feature["source_variant"],
            func=get,
        )

    def feature_df_from_transformation(self, feature):
        df = self.process_transformation(
            feature["source_name"], feature["source_variant"]
        )
        if isinstance(df, pd.Series):
            df = df.to_frame()
            df.reset_index(inplace=True)
        if feature["source_timestamp"] != "" and feature["source_timestamp"] not in df:
            raise ValueError(
                f"Provided timestamp column '{feature['source_timestamp']}' for feature "
                f"'{feature['name']}:{feature['variant']}' not found in source '{feature['source_name']}:{feature['source_variant']}'; "
                f"either remove 'timestamp_column' from the feature registration or include it in the source."
            )
        elif feature["source_timestamp"] != "":
            df = df[
                [
                    feature["source_entity"],
                    feature["source_value"],
                    feature["source_timestamp"],
                ]
            ]
            df[feature["source_timestamp"]] = pd.to_datetime(
                df[feature["source_timestamp"]]
            )
        else:
            df = df[[feature["source_entity"], feature["source_value"]]]
        return df

    def features(
        self,
        feature_variant_list,
        entities: Dict,
        model: Union[str, Model] = None,
        params: list = None,
    ):
        if len(feature_variant_list) == 0:
            raise Exception("No features provided")

        self.entities = entities
        self.params = params if params else []

        self.__validate_entity_exists(entities, feature_variant_list)

        entity_name = list(entities.keys())[0] if len(entities) > 0 else ""
        entity_value = entities[entity_name] if len(entities) > 0 else ""
        features = self.add_features_to_list(
            feature_variant_list, entity_name, entity_value
        )
        # all_features_df = list_to_combined_df(all_features_list, entity_name)
        # features = get_features_for_entity(entity_name, entity_value, all_features_df)

        if model is not None:
            for feature_name, feature_variant in feature_variant_list:
                self._register_model(
                    model,
                    look_up_table="model_features",
                    association_name=feature_name,
                    association_variant=feature_variant,
                )

        return features

    def __validate_entity_exists(self, entities, feature_variant_list):
        # validate entities exists if any of the features are not ondemand
        if any(
            [
                self.db.get_feature_variant_mode(f_name, f_variant)
                != ComputationMode.CLIENT_COMPUTED
                for f_name, f_variant in feature_variant_list
            ]
        ):
            if len(entities) == 0:
                raise Exception("Entities are required for features (unless ondemand)")

    def calculate_ondemand_feature(self, f_name, f_variant):
        query = self.db.get_ondemand_feature_query(f_name, f_variant)
        base64_bytes = query.encode("ascii")
        query = base64.b64decode(base64_bytes)

        code = dill.loads(bytearray(query))
        func = types.FunctionType(code, globals(), "transformation")
        return func(self, self.params, self.entities)

    def add_features_to_list(self, feature_variant_list, entity_name, entity_value):
        feature_list = []

        for feature_variant in feature_variant_list:
            f_name = feature_variant[0]
            f_variant = feature_variant[1]
            f_mode = self.db.get_feature_variant_mode(f_name, f_variant)

            if f_mode == ComputationMode.CLIENT_COMPUTED:
                output_value = self.calculate_ondemand_feature(f_name, f_variant)
                feature_list.append(output_value)
            else:
                self.compute_feature(f_name, f_variant, entity_name)
                feature_df = self.get_feature_value(f_name, f_variant, entity_value)

                feature_list.append(feature_df)

        return np.array(feature_list)

    def compute_feature(self, f_name, f_variant, entity_name):
        feature = self.db.get_feature_variant(f_name, f_variant)
        source_name, source_variant = feature["source_name"], feature["source_variant"]

        source_files_from_db = self.db.get_source_files_for_resource(
            "transformation", source_name, source_variant
        )

        provider_obj = metadata.get_provider(feature["provider"])
        provider_type = provider_obj.function
        # This will be replaced to select the appropriate provider for each feature
        provider = get_provider(provider_type)(provider_obj.config)
        table_exists = provider.table_exists(f_name, f_variant)

        if (
            not any(
                self._file_has_changed(
                    source_file["updated_at"], source_file["file_path"]
                )
                for source_file in source_files_from_db
            )
            and len(source_files_from_db) > 0
            and table_exists
        ):
            return

        if feature["entity"] != entity_name:
            raise ValueError(
                f"Invalid entity {entity_name} for feature {source_name}-{source_variant}"
            )
        if (
            self.db.is_transformation(source_name, source_variant)
            != SourceType.PRIMARY_SOURCE.value
        ):
            feature_df = self.process_non_primary_df_transformation(
                feature, source_name, source_variant, entity_name
            )
        else:
            source = self.db.get_source_variant(source_name, source_variant)
            feature_df = feature_df_with_entity(
                source["definition"], entity_name, feature
            )

        if table_exists:
            table = provider.get_table(f_name, f_variant)
        else:
            if not feature["is_embedding"]:
                table = provider.create_table(
                    f_name, f_variant, Scalar(ScalarType(feature["data_type"]))
                )
            else:
                table = provider.create_index(
                    f_name,
                    f_variant,
                    VectorType(
                        ScalarType(feature["data_type"]), feature["dimension"], True
                    ),
                )

        total = len(feature_df)
        if provider_type == "LOCAL_ONLINE":
            table.set_batch(feature_df)
        else:
            for index, row in feature_df.iterrows():
                table.set(row[0], row[1])
                progress_bar(
                    total,
                    index,
                    prefix="Updating Feature Table:",
                    suffix="Complete",
                    length=50,
                )
        progress_bar(
            total, total, prefix="Updating Feature Table:", suffix="Complete", length=50
        )
        print("\n")

    @staticmethod
    def _file_has_changed(last_updated_at, file_path):
        """
        Currently using last updated at for determining if a file has changed. We can consider using the file hash
        if this becomes a performance issue.
        """
        os_last_updated = os.path.getmtime(file_path)
        return os_last_updated > float(last_updated_at)

    def get_feature_value(self, f_name, f_variant, entity_value):
        feature = self.db.get_feature_variant(f_name, f_variant)
        provider_obj = metadata.get_provider(feature["provider"])
        provider_type = provider_obj.function
        provider = get_provider(provider_type)(provider_obj.config)
        table = provider.get_table(f_name, f_variant)
        value = table.get(entity_value)

        return value

    def process_non_primary_df_transformation(
        self, feature, source_name, source_variant, entity_id
    ):
        name_variant = f"{feature['name']}.{feature['variant']}"
        feature_df = self.process_transformation(source_name, source_variant)
        if isinstance(feature_df, pd.Series):
            feature_df = feature_df.to_frame()
            feature_df.reset_index(inplace=True)
        if not feature["source_entity"] in feature_df.columns:
            raise ValueError(
                f"Could not set entity column. No column name {feature['source_entity']} exists in {source_name} ({source_variant})"
            )
        if not feature["source_value"] in feature_df.columns:
            raise ValueError(
                f"Could not access feature value column. No column name '{feature['source_value']}' exists in {source_name} ({source_variant})"
            )
        feature_df = feature_df[[feature["source_entity"], feature["source_value"]]]
        feature_df.rename(
            columns={
                feature["source_entity"]: entity_id,
                feature["source_value"]: name_variant,
            },
            inplace=True,
        )
        feature_df.drop_duplicates(subset=[entity_id], keep="last", inplace=True)
        feature_df.set_index(entity_id)
        return feature_df

    @staticmethod
    def convert_ts_df_to_dataset(label_row, trainingset_df, include_label_timestamp):
        if label_row["source_timestamp"] != "" and include_label_timestamp != True:
            trainingset_df.drop(columns=label_row["source_timestamp"], inplace=True)
        elif label_row["source_timestamp"] != "" and include_label_timestamp:
            source_timestamp_col = trainingset_df.pop(label_row["source_timestamp"])
            trainingset_df = trainingset_df.assign(label_timestamp=source_timestamp_col)
        trainingset_df.drop(columns=label_row["source_entity"], inplace=True)

        label_col = trainingset_df.pop("label")
        trainingset_df = trainingset_df.assign(label=label_col)
        return Dataset.from_dataframe(trainingset_df, include_label_timestamp)

    def _register_model(
        self,
        model: Union[str, Model],
        look_up_table: str,
        association_name: str,
        association_variant: str,
    ):
        name = model if isinstance(model, str) else model.name
        type = "Model" if isinstance(model, str) else model.type()

        self.db.insert("models", name, type)
        self.db.insert(look_up_table, name, association_name, association_variant)

    def _get_source_as_df(self, name, variant, limit):
        if limit == 0:
            raise ValueError("limit must be greater than 0")
        df = self.get_input_df(name, variant)
        if limit != NO_RECORD_LIMIT:
            return df[:limit]
        else:
            return df

    def nearest(self, name, variant, vector, k):
        feature = self.db.get_feature_variant(name, variant)
        self.compute_feature(name, variant, feature["entity"])
        provider_obj = metadata.get_provider(feature["provider"])
        provider_type = provider_obj.function
        provider = get_provider(provider_type)(provider_obj.config)

        if provider.table_exists(name, variant):
            table = provider.get_table(name, variant)
        else:
            raise ValueError(f"Table does not exist for feature {name} ({variant})")
        return table.nearest(name, variant, vector, k)

    def close(self):
        self.db.close()


class Stream:
    def __init__(self, stub, name, version, model: Union[str, Model] = None):
        req = serving_pb2.TrainingDataRequest()
        req.id.name = name
        req.id.version = version
        if model is not None:
            req.model.name = model if isinstance(model, str) else model.name
        self.name = name
        self.version = version
        self._stub = stub
        self._req = req
        self._iter = stub.TrainingData(req)

    def __iter__(self):
        return self

    def __next__(self):
        return Row(next(self._iter))

    def restart(self):
        self._iter = self._stub.TrainingData(self._req)


class LocalStream:
    def __init__(self, datalist, include_label_timestamp):
        self._datalist = datalist
        self._iter = iter(datalist)
        self._include_label_timestamp = include_label_timestamp

    def __iter__(self):
        return iter(self._iter)

    def __next__(self):
        return LocalRow(next(self._iter), self._include_label_timestamp)

    def restart(self):
        self._iter = iter(self._datalist)


class Repeat:
    def __init__(self, repeat_num, stream):
        self.repeat_num = repeat_num
        self._stream = stream

    def __iter__(self):
        return self

    def __next__(self):
        try:
            next_val = next(self._stream)
        except StopIteration:
            self.repeat_num -= 1
            if self.repeat_num >= 0:
                self._stream.restart()
                next_val = next(self._stream)
            else:
                raise

        return next_val


class Shuffle:
    def __init__(self, buffer_size, stream):
        self.buffer_size = buffer_size
        self._shuffled_data_list = []
        self._stream = stream
        self.__setup_buffer()

    def __setup_buffer(self):
        try:
            for _ in range(self.buffer_size):
                self._shuffled_data_list.append(next(self._stream))
        except StopIteration:
            pass

    def restart(self):
        self._stream.restart()
        self.__setup_buffer()

    def __iter__(self):
        return self

    def __next__(self):
        if len(self._shuffled_data_list) == 0:
            raise StopIteration
        random_index = random.randrange(len(self._shuffled_data_list))
        next_row = self._shuffled_data_list.pop(random_index)

        try:
            self._shuffled_data_list.append(next(self._stream))
        except StopIteration:
            pass

        return next_row


class Batch:
    def __init__(self, batch_size, stream):
        self.batch_size = batch_size
        self._stream = stream

    def restart(self):
        self._stream.restart()

    def __iter__(self):
        return self

    def __next__(self):
        rows = BatchRow()
        for _ in range(self.batch_size):
            try:
                next_row = next(self._stream)
                rows.append(next_row)
            except StopIteration:
                if len(rows) == 0:
                    raise
                return rows
        return rows


class Dataset:
    def __init__(self, stream, dataframe=None):
        """Repeats the Dataset for the specified number of times

        Args:
            stream (Iterator): An iterable object.

        Returns:
            self (Dataset): Returns a Dataset created from the iterable object.
        """
        self._stream = stream
        self._dataframe = dataframe

    def from_stub(self, name, version, model: Union[str, Model] = None):
        stream = Stream(self._stream, name, version, model)
        return Dataset(stream)

    def dataframe(self, spark_session=None):
        """Returns the training set as a Pandas DataFrame or Spark DataFrame.
        Args:
        - spark_session: Optional(SparkSession)


        **Examples**:
        ``` py
            client = Client()
            df = client.training_set("fraud_training", "v1").dataframe()
            print(df.head())
            # Output:
            #                feature_avg_transactions_default             label
            # 0                                25.0                       False
            # 1                             27999.0                       False
            # 2                               459.0                       False
            # 3                              2060.0                        True
            # 4                              1762.5                       False
        ```

        Returns:
            df: Union[pd.DataFrame, pyspark.sql.DataFrame] A DataFrame containing the training set.
        """

        if self._dataframe is not None:
            return self._dataframe
        elif spark_session is not None:
            req = serving_pb2.TrainingDataRequest()
            req.id.name = self._stream.name
            req.id.version = self._stream.version
            resp = self._stream._stub.ResourceLocation(req)

            file_format = FileFormat.get_format(resp.location, default="parquet")
            location = self._sanitize_location(resp.location)
            self._dataframe = self._get_spark_dataframe(
                spark_session, file_format, location
            )
            return self._dataframe
        else:
            name = self._stream.name
            variant = self._stream.version
            stub = self._stream._stub
            id = serving_pb2.TrainingDataID(name=name, version=variant)
            req = serving_pb2.TrainingDataRequest(id=id)
            cols = stub.TrainingDataColumns(req)
            data = [r.to_dict(cols.features, cols.label) for r in self._stream]
            self._dataframe = pd.DataFrame(
                data=data, columns=[*cols.features, cols.label]
            )
            self._dataframe.rename(columns={cols.label: "label"}, inplace=True)
            return self._dataframe

    def _sanitize_location(self, location: str) -> str:
        # Returns the location directory rather than a single file if it is part-file.
        # Also converts s3:// to s3a:// if necessary.

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

        return location

    def _get_spark_dataframe(self, spark, file_format, location):
        if file_format not in FileFormat.supported_formats():
            raise Exception(
                f"file type '{file_format}' is not supported. Please use 'csv' or 'parquet'"
            )

        try:
            df = (
                spark.read.option("header", "true")
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

    def from_dataframe(dataframe, include_label_timestamp):
        stream = LocalStream(dataframe.values.tolist(), include_label_timestamp)
        return Dataset(stream, dataframe)

    def pandas(self):
        warnings.warn(
            "Dataset.pandas() is deprecated and will be removed in future versions; use Dataset.dataframe() instead.",
            PendingDeprecationWarning,
        )
        if self._dataframe is None:
            _ = self.dataframe()
        return self._dataframe

    def repeat(self, num):
        """Repeats the Dataset for the specified number of times

        **Examples**:
        ``` py
            client = ff.Client()
            dataset = client.training_set("fraud_training", "quickstart")
            training_dataset = dataset.repeat(10) # Repeats data 10 times
            for feature_batch in training_dataset:
                # Train model
        ```

        Args:
            num (int): The number of times the dataset will be repeated

        Returns:
            self (Dataset): Returns the current Dataset
        """
        if num <= 0:
            raise Exception("Must repeat 1 or more times")
        self._stream = Repeat(num, self._stream)
        if self._dataframe is not None:
            temp_df = self._dataframe
            for i in range(num):
                self._dataframe = pd.concat([self._dataframe, temp_df])
        return self

    def shuffle(self, buffer_size):
        """Swaps random rows within the Dataset.

        **Examples**:
        ``` py
            client = ff.Client()
            dataset = client.training_set("fraud_training", "quickstart")
            training_dataset = dataset.shuffle(100) # Swaps 100 Rows
            for feature_batch in training_dataset:
                # Train model
        ```

        Args:
            buffer_size (int): The number of Dataset rows to be randomly swapped

        Returns:
            self (Dataset): Returns the current Dataset
        """
        if buffer_size <= 0:
            raise Exception("Buffer size must be greater than or equal to 1")
        self._stream = Shuffle(buffer_size, self._stream)
        if self._dataframe is not None:
            self._dataframe = self._dataframe.sample(frac=1)
        return self

    def batch(self, batch_size):
        """Creates a batch row in the Dataset.

        **Examples**:
        ``` py
            client = ff.Client()
            dataset = client.training_set("fraud_training", "quickstart")
            training_dataset = dataset.batch(8) # Creates a batch of 8 Datasets for each row
            for feature_batch in training_dataset:
                # Train model
        ```

        Args:
            batch_size (int): The number of items to be added to each batch

        Returns:
            self (Dataset): Returns the current Dataset
        """
        if batch_size <= 0:
            raise Exception("Batch size must be greater than or equal to 1")
        self._stream = Batch(batch_size, self._stream)
        if self._dataframe is not None:
            self._dataframe = np.array_split(
                self._dataframe, math.ceil(len(self._dataframe) // batch_size)
            )
        return self

    def __iter__(self):
        return self

    def __next__(self):
        next_val = next(self._stream)
        return next_val


class Row:
    def __init__(self, proto_row):
        self._features = np.array(
            [parse_proto_value(feature) for feature in proto_row.features]
        )
        self._label = parse_proto_value(proto_row.label)
        self._row = np.append(self._features, self._label)

    def features(self):
        return [self._row[:-1]]

    def label(self):
        return [self._label]

    def to_numpy(self):
        return self._row

    def to_dict(self, feature_columns: List[str], label_column: str):
        row_dict = dict(zip(feature_columns, self._features))
        row_dict[label_column] = self._label
        return row_dict

    def __repr__(self):
        return "Features: {} , Label: {}".format(self.features(), self.label())


class LocalRow:
    def __init__(self, row_list, include_label_timestamp):
        """
        If include_label_timestamp is true then we want the label to equal to the
        last two columns in the list. Otherwise, just the label will be the last column only.
        """

        self._features = row_list[:-2] if include_label_timestamp else row_list[:-1]
        self._row = row_list
        self._label = row_list[-2:] if include_label_timestamp else row_list[-1]

    def features(self):
        return [self._features]

    def label(self):
        return [self._label]

    def to_numpy(self):
        return np.array(self._row)

    def __repr__(self):
        return "Features: {} , Label: {}".format(self.features(), self.label())


class BatchRow:
    def __init__(self, rows=None):
        self._features = []
        self._labels = []
        if rows is None:
            rows = []
        self._rows = rows
        for row in rows:
            self.append(row)

    def append(self, row):
        self._features.append(row.features()[0])
        self._labels.append(row.label()[0])
        self._rows.append(row)

    def features(self):
        return self._features

    def label(self):
        return self._labels

    def to_list(self):
        return self._rows

    def __len__(self):
        return len(self._rows)


def parse_proto_value(value):
    """parse_proto_value is used to parse the one of Value message"""
    return getattr(value, value.WhichOneof("value"))


class FeatureSetIterator:
    def __init__(self, stub, features):
        req = serving_pb2.BatchFeatureServeRequest()
        for name, variant in features:
            feature_id = req.features.add()
            feature_id.name = name
            feature_id.version = variant
        self._stub = stub
        self._req = req
        self._iter = stub.BatchFeatureServe(req)

    def __iter__(self):
        return self

    def __next__(self):
        return FeatureSetRow(next(self._iter)).to_tuple()

    def restart(self):
        self._iter = self._stub.BatchFeatureServe(self._req)


class FeatureSetRow:
    def __init__(self, proto_row):
        self._features = [parse_proto_value(feature) for feature in proto_row.features]
        self._entity = parse_proto_value(proto_row.entity)
        self._row = [self._entity, self._features]

    def features(self):
        return self._row[1]

    def entity(self):
        return [self._entity]

    def to_numpy(self):
        return np.array(self._row)

    def to_tuple(self):
        return tuple((self._entity, self._features))

    def to_dict(self, feature_columns: List[str], entity_column: str):
        row_dict = dict(zip(feature_columns, self._features))
        row_dict[entity_column] = self._entity
        return row_dict

    def __repr__(self):
        return "Features: {} , Entity: {}".format(self.features(), self.entity())


# Create a featureset row class
# modify restart

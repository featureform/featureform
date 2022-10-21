# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
import json
import dill
import os
import random
import re
import types
import math

import grpc
import numpy as np
from featureform.proto import serving_pb2
from featureform.proto import serving_pb2_grpc
from .tls import insecure_channel, secure_channel
import pandas as pd
from pandasql import sqldf
from .sqlite_metadata import SQLiteMetadata
from .resources import SourceType


def check_feature_type(features):
    checked_features = []
    for feature in features:
        if isinstance(feature, tuple):
            checked_features.append(feature)
        elif isinstance(feature, str):
            checked_features.append((feature, "default"))
    return checked_features


class ServingClient:
    """
    The serving client is used to retrieve training sets and features for training and serving purposes.


    **Using the Serving Client:**
    ``` py
    import featureform as ff
    from featureform import ServingClient

    client = ServingClient(host="localhost:8000")

    # example:
    dataset = client.training_set("fraud_training", "quickstart")
    training_dataset = dataset.repeat(10).shuffle(1000).batch(8)
    for feature_batch in training_dataset:
        # Train model
    ```
    """

    def __init__(self, host=None, local=False, insecure=False, cert_path=None):
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

    def training_set(self, name, variant="default", include_label_timestamp=False):
        """Return an iterator that iterates through the specified training set.

        **Examples**:
        ``` py
            client = ff.ServingClient()
            dataset = client.training_set("fraud_training", "quickstart")
            training_dataset = dataset.repeat(10).shuffle(1000).batch(8)
            for feature_batch in training_dataset:
                # Train model
        ```
        Args:
            name (str): Name of training set to be retrieved
            variant (str): Variant of training set to be retrieved

        Returns:
            training set (Dataset): A training set iterator
        """
        return self.impl.training_set(name, variant, include_label_timestamp)

    def features(self, features, entities):
        """Returns the feature values for the specified entities.

        **Examples**:
        ``` py
            client = ff.ServingClient(local=True)
            fpf = client.features([("avg_transactions", "quickstart")], {"CustomerID": "C1410926"})
            # Run features through model
        ```
        Args:
            features (list[(str, str)], list[str]): List of Name Variant Tuples
            entities (dict): Dictionary of entity name/value pairs

        Returns:
            features (numpy.Array): An Numpy array of feature values in the order given by the inputs
        """
        features = check_feature_type(features)
        return self.impl.features(features, entities)


class HostedClientImpl:
    def __init__(self, host=None, insecure=False, cert_path=None):
        host = host or os.getenv('FEATUREFORM_HOST')
        if host is None:
            raise ValueError(
                'If not in local mode then `host` must be passed or the environment'
                ' variable FEATUREFORM_HOST must be set.'
            )
        channel = self._create_channel(host, insecure, cert_path)
        self._stub = serving_pb2_grpc.FeatureStub(channel)

    def _create_channel(self, host, insecure, cert_path):
        if insecure:
            return insecure_channel(host)
        else:
            return secure_channel(host, cert_path)

    def training_set(self, name, variation, include_label_timestamp):
        return Dataset(self._stub).from_stub(name, variation)

    def features(self, features, entities):
        req = serving_pb2.FeatureServeRequest()
        for name, value in entities.items():
            entity_proto = req.entities.add()
            entity_proto.name = name
            entity_proto.value = value
        for (name, variation) in features:
            feature_id = req.features.add()
            feature_id.name = name
            feature_id.version = variation
        resp = self._stub.FeatureServe(req)
        return [parse_proto_value(val) for val in resp.values]


class LocalClientImpl:
    def __init__(self):
        self.db = SQLiteMetadata()

    def training_set(self, training_set_name, training_set_variant, include_label_timestamp):
        training_set = self.db.get_training_set_variant(training_set_name, training_set_variant)
        label = self.db.get_label_variant(training_set['label_name'], training_set['label_variant'])
        label_df = self.get_label_dataframe(label)
        # We will build the training set DF by merging each feature one by one into it.
        trainingset_df = label_df
        features = self.db.get_training_set_features(training_set_name, training_set_variant)
        for feature_variant in features:
            feature = self.db.get_feature_variant(feature_variant['feature_name'], feature_variant['feature_variant'])
            feature_df = self.get_feature_dataframe(feature)
            trainingset_df = self.merge_feature_into_ts(feature, label, feature_df, trainingset_df)

        return self.convert_ts_df_to_dataset(label, trainingset_df, include_label_timestamp)

    def get_input_df(self, source_name, source_variant):
        if self.db.is_transformation(source_name, source_variant) == SourceType.PRIMARY_SOURCE.value:
            source = self.db.get_source_variant(source_name, source_variant)
            df = pd.read_csv(str(source['definition']))
        else:
            df = self.process_transformation(source_name, source_variant)
        return df

    def sql_transformation(self, query):
        # Use regex to parse query inputs in double curly braces {{ }} and store in a list
        inputs = re.findall("(?={{).*?(?<=}})", query)
        for i, input in enumerate(inputs):
            # Trim curly braces before and after to get name.variant from {{name.variant}}
            name_variant = input[2:-2].split(".")
            source_name, source_variant = name_variant[0], name_variant[1]
            # Creates a variable called dataframes_i which stores the corresponding df for each input
            df_variable = f"dataframes_{i}"
            # globals()[df_variable]:
            # 1. Converts a string "dataframes_i" to a variable name
            # 2. Assigns a global scope to the variable, to access it outside the loop
            globals()[df_variable] = self.get_input_df(source_name, source_variant)
            query = query.replace(input, df_variable)

        return sqldf(query, globals())

    def process_transformation(self, name, variant):
        source = self.db.get_source_variant(name, variant)
        if self.db.is_transformation(name, variant) == SourceType.SQL_TRANSFORMATION.value:
            query = source['definition']
            new_data = self.sql_transformation(query)
        else:
            code = dill.loads(bytearray(source['definition']))
            inputs = json.loads(source['inputs'])
            dataframes = []
            for input in inputs:
                source_name, source_variant = input[0], input[1],
                dataframes.append(self.get_input_df(source_name, source_variant))
            func = types.FunctionType(code, globals(), "transformation")
            new_data = func(*dataframes)

        return new_data

    def get_label_dataframe(self, label):
        transform_type = self.db.is_transformation(label['source_name'], label['source_variant'])
        if transform_type == SourceType.SQL_TRANSFORMATION.value or transform_type == SourceType.DF_TRANSFORMATION.value:
            label_df = self.label_df_from_transformation(label)
        else:
            label_source = self.db.get_source_variant(label['source_name'], label['source_variant'])
            label_df = self.label_df_from_csv(label, label_source['definition'])
        label_df.rename(columns={label['source_value']: 'label'}, inplace=True)
        return label_df

    def label_df_from_transformation(self, label):
        df = self.process_transformation(label['source_name'], label['source_variant'])
        if label['source_timestamp'] != "":
            df = df[[label['source_entity'], label['source_value'], label['source_timestamp']]]
            df[label['source_timestamp']] = pd.to_datetime(df[label['source_timestamp']])
        else:
            df = df[[label['source_entity'], label['source_value']]]
        df.set_index(label['source_entity'])
        return df

    def label_df_from_csv(self, label, file_name):
        df = pd.read_csv(file_name)
        self.check_missing_values(label, df)
        if label['source_timestamp'] != "":
            df = df[[label['source_entity'], label['source_value'], label['source_timestamp']]]
            df[label['source_timestamp']] = pd.to_datetime(df[label['source_timestamp']])
            df.sort_values(by=label['source_timestamp'], inplace=True)
            df.drop_duplicates(subset=[label['source_entity'], label['source_timestamp']], keep="last", inplace=True)
        else:
            df = df[[label['source_entity'], label['source_value']]]
        df.set_index(label['source_entity'], inplace=True)
        return df

    def get_feature_dataframe(self, feature):
        name_variant = feature['name'] + "." + feature['variant']
        transform_type = self.db.is_transformation(feature['source_name'], feature['source_variant'])
        if transform_type == SourceType.SQL_TRANSFORMATION.value or transform_type == SourceType.DF_TRANSFORMATION.value:
            feature_df = self.feature_df_from_transformation(feature)
        else:
            source = self.db.get_source_variant(feature['source_name'], feature['source_variant'])
            feature_df = self.feature_df_from_csv(feature, source['definition'])
        feature_df.set_index(feature['source_entity'])
        feature_df.rename(columns={feature['source_value']: name_variant}, inplace=True)
        return feature_df

    def feature_df_from_transformation(self, feature):
        df = self.process_transformation(feature['source_name'], feature['source_variant'])
        if isinstance(df, pd.Series):
            df = df.to_frame()
            df.reset_index(inplace=True)
        if feature['source_timestamp'] != "":
            df = df[[feature['source_entity'], feature['source_value'], feature['source_timestamp']]]
            df[feature['source_timestamp']] = pd.to_datetime(df[feature['source_timestamp']])
        else:
            df = df[[feature['source_entity'], feature['source_value']]]
        return df

    def feature_df_from_csv(self, feature, filename):
        df = pd.read_csv(str(filename))
        self.check_missing_values(feature, df)
        if feature['source_timestamp'] != "":
            df = df[[feature['source_entity'], feature['source_value'], feature['source_timestamp']]]
            df[feature['source_timestamp']] = pd.to_datetime(df[feature['source_timestamp']])
            df = df.sort_values(by=feature['source_timestamp'], ascending=True)
        else:
            df = df[[feature['source_entity'], feature['source_value']]]
        return df

    def merge_feature_into_ts(self, feature_row, label_row, df, trainingset_df):
        if feature_row['source_timestamp'] != "":
            trainingset_df = pd.merge_asof(trainingset_df, df.sort_values(feature_row['source_timestamp']),
                                           direction='backward',
                                           left_on=label_row['source_timestamp'],
                                           right_on=feature_row['source_timestamp'], left_by=label_row['source_entity'],
                                           right_by=feature_row['source_entity'])
            if feature_row['source_timestamp'] != label_row['source_timestamp']:
                trainingset_df.drop(columns=feature_row['source_timestamp'], inplace=True)
        else:
            df.drop_duplicates(subset=[feature_row['source_entity']], keep="last", inplace=True)
            trainingset_df.reset_index(inplace=True)
            trainingset_df[label_row['source_entity']] = trainingset_df[label_row['source_entity']].astype('string')
            df[label_row['source_entity']] = df[label_row['source_entity']].astype('string')
            trainingset_df = trainingset_df.join(df.set_index(label_row['source_entity']), how="left",
                                                 on=label_row['source_entity'],
                                                 lsuffix="_left")
            if "index" in trainingset_df.columns:
                trainingset_df.drop(columns='index', inplace=True)
        return trainingset_df

    def convert_ts_df_to_dataset(self, label_row, trainingset_df, include_label_timestamp): 
        if label_row['source_timestamp'] != "" and include_label_timestamp != True:
            trainingset_df.drop(columns=label_row['source_timestamp'], inplace=True)
        elif label_row['source_timestamp'] != "" and include_label_timestamp:
            source_timestamp_col = trainingset_df.pop(label_row['source_timestamp'])
            trainingset_df = trainingset_df.assign(label_timestamp=source_timestamp_col)
        trainingset_df.drop(columns=label_row['source_entity'], inplace=True)
            
        label_col = trainingset_df.pop('label')
        trainingset_df = trainingset_df.assign(label=label_col)
        return Dataset.from_dataframe(trainingset_df, include_label_timestamp)

    def features(self, feature_variant_list, entities):
        if len(feature_variant_list) == 0:
            raise Exception("No features provided")
        # This code assumes that the entities dictionary only has one entity
        entity_id = list(entities.keys())[0]
        entity_value = entities[entity_id]
        all_features_list = self.add_feature_dfs_to_list(feature_variant_list, entity_id)
        all_features_df = self.list_to_combined_df(all_features_list, entity_id)
        return self.get_features_for_entity(entity_id, entity_value, all_features_df)

    def add_feature_dfs_to_list(self, feature_variant_list, entity_id):
        feature_df_list = []
        for feature_variant in feature_variant_list:
            feature = self.db.get_feature_variant(feature_variant[0], feature_variant[1])
            name_variant = f"{feature['name']}.{feature['variant']}"
            source_name, source_variant = feature['source_name'], feature['source_variant']
            if feature["entity"] != entity_id:
                raise ValueError(
                    f"Invalid entity {entity_id} for feature {source_name}-{source_variant}")
            if self.db.is_transformation(source_name, source_variant) != SourceType.PRIMARY_SOURCE.value:
                feature_df = self.process_transformation(source_name, source_variant)
                if isinstance(feature_df, pd.Series):
                    feature_df = feature_df.to_frame()
                    feature_df.reset_index(inplace=True)
                if not feature["source_entity"] in feature_df.columns:
                    raise ValueError(
                        f"Could not set entity column. No column name {feature['source_entity']} exists in {source_name}-{source_variant}")
                if not feature['source_value'] in feature_df.columns:
                    raise ValueError(
                        f"Could not access feature value column. No column name {feature['source_value']} exists in {source_name}-{source_variant}")
                feature_df = feature_df[[feature['source_entity'], feature['source_value']]]
                feature_df.rename(columns={feature['source_entity']: entity_id, feature['source_value']: name_variant}, inplace=True)
                feature_df.drop_duplicates(subset=[entity_id], keep="last", inplace=True)
                feature_df.set_index(entity_id)
            else:
                source = self.db.get_source_variant(source_name, source_variant)
                feature_df = self.feature_df_with_entity(source['definition'], entity_id, feature)
            feature_df_list.append(feature_df)

        return feature_df_list

    def list_to_combined_df(self, features_list, entity_id):
        all_feature_df = None
        try:
            for feature in features_list:
                if all_feature_df is None:
                    all_feature_df = feature
                else:
                    all_feature_df = all_feature_df.join(feature.set_index(entity_id), on=entity_id,
                                                         lsuffix='_left')
            return all_feature_df
        except TypeError:
            print("Set is empty")

    def get_features_for_entity(self, entity_id, entity_value, all_feature_df):
        entity = all_feature_df.loc[all_feature_df[entity_id] == entity_value].copy()
        entity.drop(columns=entity_id, inplace=True)
        if len(entity.values) > 0:
            return entity.values[0]
        else:
            raise Exception(f"No matching entities for {entity_id}: {entity_value}")

    def feature_df_with_entity(self, source_path, entity_id, feature):
        name_variant = f"{feature['name']}.{feature['variant']}"
        df = pd.read_csv(str(source_path))
        self.check_missing_values(feature, df)
        if feature['source_timestamp'] != "":
            df = df[[feature['source_entity'], feature['source_value'], feature['source_timestamp']]]
            df = df.sort_values(by=feature['source_timestamp'], ascending=True)
            df = df.drop(columns=feature['source_timestamp'])
        else:
            df = df[[feature['source_entity'], feature['source_value']]]
        df.set_index(feature['source_entity'])
        df.rename(columns={feature['source_entity']: entity_id, feature['source_value']: name_variant}, inplace=True)
        df.drop_duplicates(subset=[entity_id], keep="last", inplace=True)
        return df

    def check_missing_values(self, resource, df):
        if resource['source_entity'] not in df.columns:
            raise KeyError(f"Entity column does not exist: {resource['source_entity']}")
        if resource['source_value'] not in df.columns:
            raise KeyError(f"Value column does not exist: {resource['source_value']}")
        if resource['source_timestamp'] not in df.columns and resource['source_timestamp'] != "":
            raise KeyError(f"Timestamp column does not exist: {resource['source_timestamp']}")


class Stream:

    def __init__(self, stub, name, version):
        req = serving_pb2.TrainingDataRequest()
        req.id.name = name
        req.id.version = version
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

    def from_stub(self, name, version):
        stream = Stream(self._stream, name, version)
        return Dataset(stream)
    
    def from_dataframe(dataframe, include_label_timestamp):
        stream = LocalStream(dataframe.values.tolist(), include_label_timestamp)
        return Dataset(stream, dataframe)

    def pandas(self):
        return self._dataframe

    def repeat(self, num):
        """Repeats the Dataset for the specified number of times

        **Examples**:
        ``` py
            client = ff.ServingClient()
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
                self._dataframe = self._dataframe.append(temp_df)
        return self

    def shuffle(self, buffer_size):
        """Swaps random rows within the Dataset.

        **Examples**:
        ``` py
            client = ff.ServingClient()
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
            client = ff.ServingClient()
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
            self._dataframe = np.array_split(self._dataframe, math.ceil(len(self._dataframe) // batch_size))
        return self

    def __iter__(self):
        return self

    def __next__(self):
        next_val = next(self._stream)
        return next_val


class Row:

    def __init__(self, proto_row):
        features = np.array(
            [parse_proto_value(feature) for feature in proto_row.features])
        self._label = parse_proto_value(proto_row.label)
        self._row = np.append(features, self._label)

    def features(self):
        return [self._row[:-1]]

    def label(self):
        return [self._label]

    def to_numpy(self):
        return self._row()

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
    """ parse_proto_value is used to parse the one of Value message
	"""
    return getattr(value, value.WhichOneof("value"))

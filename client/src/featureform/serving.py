# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
import json
import marshal
import os
import random
import types

import grpc
import numpy as np
from featureform.proto import serving_pb2
from featureform.proto import serving_pb2_grpc
import pandas as pd
from .sqlite_metadata import SQLiteMetadata


class Client:

    def __init__(self, host=None, local=False, tls_verify=True, cert_path=None):
        self.local = local
        if local:
            if host != None:
                raise ValueError("Cannot be local and have a host")
            self.sqldb = SQLiteMetadata()
        else:
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
            self._stub = serving_pb2_grpc.FeatureStub(channel)

    def training_set(self, name, version):
        if self.local:
            return self._local_training_set(name, version)
        else:
            return self._host_training_set(name, version)

    def _host_training_set(self, name, version):
        if self.local:
            raise ValueError("Not supported in localmode. Please try using training_set()")
        return Dataset(self._stub).from_stub(name, version)

    def _local_training_set(self, trainingSetName, trainingSetVariant):
        if not self.local:
            raise ValueError("Only supported in localmode. Please try using dataset()")
        training_set_row = \
            self.sqldb.getNameVariant("training_set_variant", "trainingSetName", trainingSetName, "variantName",
                                      trainingSetVariant)[0]
        label_row = \
            self.sqldb.getNameVariant("labels_variant", "labelName", training_set_row['labelName'], "variantName",
                                      training_set_row['labelVariant'])[0]
        label_source = self.sqldb.getNameVariant("source_variant", "name", label_row['sourceName'], "variant", label_row['sourceVariant'])[0]
        if self.sqldb.is_transformation(label_row['sourceName'], label_row['sourceVariant']):
            df = self.process_transformation(label_row['sourceName'], label_row['sourceVariant'])
            if label_row['sourceTimestamp'] != "":
                df = df[[label_row['sourceEntity'], label_row['sourceValue'], label_row['sourceTimestamp']]]
            else:
                df = df[[label_row['sourceEntity'], label_row['sourceValue']]]
            df.set_index(label_row['sourceEntity'])
            label_df = df
        else:
            label_df = self.process_label_csv(label_source['definition'], label_row['sourceEntity'], label_row['sourceEntity'], label_row['sourceValue'], label_row['sourceTimestamp'])
        feature_table = self.sqldb.getNameVariant("training_set_features", "trainingSetName", trainingSetName,
                                                 "trainingSetVariant", trainingSetVariant)

        label_df.rename(columns={label_row['sourceValue']: 'label'}, inplace=True)
        trainingset_df = label_df
        for feature_variant in feature_table:
            feature_row = self.sqldb.getNameVariant("feature_variant", "featureName", feature_variant['featureName'], "variantName",
                                                    feature_variant['featureVariant'])[0]

            source_row = \
                self.sqldb.getNameVariant("source_variant", "name", feature_row['sourceName'], "variant", feature_row['sourceVariant'])[0]

            name_variant = feature_variant['featureName'] + "." + feature_variant['featureVariant']
            if self.sqldb.is_transformation(feature_row['sourceName'], feature_row['sourceVariant']):
                df = self.process_transformation(feature_row['sourceName'], feature_row['sourceVariant'])
                if isinstance(df, pd.Series):
                    df = df.to_frame()
                    df.reset_index(inplace=True)
                if feature_row['sourceTimestamp'] != "":
                    df = df[[feature_row['sourceEntity'], feature_row['sourceValue'], feature_row['sourceTimestamp']]]
                else:
                    df = df[[feature_row['sourceEntity'], feature_row['sourceValue']]]

                df.set_index(feature_row['sourceEntity'])
                df.rename(columns={feature_row['sourceValue']: name_variant}, inplace=True)
            else:
                df = pd.read_csv(str(source_row['definition']))
                if feature_variant['featureName'] != "":
                    df = df[[feature_row['sourceEntity'], feature_row['sourceValue'], feature_row['sourceTimestamp']]]
                else:
                    df = df[[feature_row['sourceEntity'], feature_row['sourceValue']]]
                df.set_index(feature_row['sourceEntity'])
                df.rename(columns={feature_row['sourceValue']: name_variant}, inplace=True)
            if feature_row['sourceTimestamp'] != "":
                trainingset_df = pd.merge_asof(trainingset_df, df.sort_values(['ts']), direction='backward',
                                               left_on=label_row['sourceTimestamp'], right_on=feature_row['sourceTimestamp'], left_by=label_row['sourceEntity'],
                                               right_by=feature_row['sourceEntity'])
            else:
                df.drop_duplicates(subset=[feature_row['sourceEntity'], name_variant])
                trainingset_df.reset_index(inplace=True)
                trainingset_df[label_row['sourceEntity']] = trainingset_df[label_row['sourceEntity']].astype('string')
                df[label_row['sourceEntity']] = df[label_row['sourceEntity']].astype('string')
                trainingset_df = trainingset_df.join(df.set_index(label_row['sourceEntity']), how="left", on=label_row['sourceEntity'],
                                                     lsuffix="_left")

        if label_row['sourceTimestamp'] != "":
            trainingset_df.drop(columns=label_row['sourceTimestamp'], inplace=True)
        trainingset_df.drop(columns=label_row['sourceEntity'], inplace=True)

        label_col = trainingset_df.pop('label')
        trainingset_df = trainingset_df.assign(label=label_col)
        return Dataset.from_list(trainingset_df.values.tolist())

    def features(self, features, entities):
        if self.local:
            return self._local_features(features, entities)
        else:
            return self._host_features(features, entities)

    def _host_features(self, features, entities):
        req = serving_pb2.FeatureServeRequest()
        for name, value in entities.items():
            entity_proto = req.entities.add()
            entity_proto.name = name
            entity_proto.value = value
        for (name, version) in features:
            feature_id = req.features.add()
            feature_id.name = name
            feature_id.version = version
        resp = self._stub.FeatureServe(req)
        return [parse_proto_value(val) for val in resp.values]

    def process_transformation(self, name, variant):
        source_row = self.sqldb.getNameVariant("source_variant", "name", name, "variant", variant)[0]
        inputs = json.loads(source_row['inputs'])
        dataframes = []
        code = marshal.loads(bytearray(source_row['definition']))
        func = types.FunctionType(code, globals(), "transformation")
        for input in inputs:
            source_name, source_variant = input[0], input[1],
            if self.sqldb.is_transformation(source_name, source_variant):
                df = self.process_transformation(source_name, source_variant)
                dataframes.append(df)
            else:
                source_row = \
                    self.sqldb.getNameVariant("source_variant", "name", source_name, "variant", source_variant)[0]
                df = pd.read_csv(str(source_row['definition']))
                dataframes.append(df)
        new_data = func(*dataframes)
        return new_data

    def _local_features(self, feature_variant_list, entity):
        if len(feature_variant_list) == 0:
            raise Exception("No features provided")
        # This code was originally written to take a tuple, this is a quick fix to turn a dict with a single entry into that tuple.
        # This should all be refactored later.
        entity_tuple = next(entity.items())
        dataframe_mapping = []
        all_feature_df = None
        for featureVariantTuple in feature_variant_list:

            feature_row = self.sqldb.getNameVariant("feature_variant", "featureName", featureVariantTuple[0],
                                                    "variantName", featureVariantTuple[1])[0]
            entity_column, ts_column, feature_column_name, source_name, source_variant = feature_row['sourceEntity'], feature_row['sourceTimestamp'], feature_row['sourceValue'], feature_row['sourceName'], feature_row['sourceVariant']

            source_row = self.sqldb.getNameVariant("source_variant", "name", source_name, "variant", source_variant)[0]
            if self.sqldb.is_transformation(source_name, source_variant):
                df = self.process_transformation(source_name, source_variant)
                if isinstance(df, pd.Series):
                    df = df.to_frame()
                    df.reset_index(inplace=True)
                df = df[[entity_tuple[0], feature_column_name]]
                df.set_index(entity_tuple[0])
                dataframe_mapping.append(df)
            else:
                name_variant = f"{featureVariantTuple[0]}.{featureVariantTuple[1]}"
                dataframe_mapping = self.process_feature_csv(source_row['definition'], entity_tuple[0], entity_column,
                                                             feature_column_name,
                                                             dataframe_mapping,
                                                             name_variant, ts_column)
        try:
            for value in dataframe_mapping:
                if all_feature_df is None:
                    all_feature_df = value
                else:
                    all_feature_df = all_feature_df.join(value.set_index(entity_tuple[0]), on=entity_tuple[0],
                                                         lsuffix='_left')
        except TypeError:
            print("Set is empty")
        entity_row = all_feature_df.loc[all_feature_df[entity_tuple[0]] == entity_tuple[1]].copy()
        entity_row.drop(columns=entity_tuple[0], inplace=True)
        if len(entity_row.values) > 0:
            return entity_row.values[0]
        else:
            raise Exception("No matching entities for {}".format(entity_tuple))

    def process_feature_csv(self, source_path, entity_name, entity_col, value_col, dataframe_mapping,
                            feature_name_variant, timestamp_column):
        df = pd.read_csv(str(source_path))
        if entity_col not in df.columns:
            raise KeyError(f"Entity column does not exist: {entity_col}")
        if value_col not in df.columns:
            raise KeyError(f"Value column does not exist: {value_col}")
        if timestamp_column != "" and timestamp_column not in df.columns:
            raise KeyError(f"Timestamp column does not exist: {timestamp_column}")
        if timestamp_column != "":
            df = df[[entity_col, value_col, timestamp_column]]
        else:
            df = df[[entity_col, value_col]]
        df.set_index(entity_col)
        if timestamp_column != "":
            df = df.sort_values(by=timestamp_column, ascending=True)
        df.rename(columns={entity_col: entity_name, value_col: feature_name_variant}, inplace=True)
        df.drop_duplicates(subset=[entity_name], keep="last", inplace=True)

        if timestamp_column != "":
            df = df.drop(columns=timestamp_column)
        dataframe_mapping.append(df)
        return dataframe_mapping

    def process_label_csv(self, source_path, entity_name, entity_col, value_col, timestamp_column):
        df = pd.read_csv(source_path)
        if entity_col not in df.columns:
            raise KeyError(f"Entity column does not exist: {entity_col}")
        if value_col not in df.columns:
            raise KeyError(f"Value column does not exist: {value_col}")
        if timestamp_column != "" and timestamp_column not in df.columns:
            raise KeyError(f"Timestamp column does not exist: {timestamp_column}")
        if timestamp_column != "":
            df = df[[entity_col, value_col, timestamp_column]]
        else:
            df = df[[entity_col, value_col]]
        if timestamp_column != "":
            df.sort_values(by=timestamp_column, inplace=True)
            df.drop_duplicates(subset=[entity_col, timestamp_column], keep="last", inplace=True)
        df.rename(columns={entity_col: entity_name}, inplace=True)
        df.set_index(entity_name, inplace=True)
        return df

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

    def __init__(self, datalist):
        self._datalist = datalist
        self._iter = iter(datalist)

    def __iter__(self):
        return iter(self._iter)

    def __next__(self):
        return LocalRow(next(self._iter))

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
        rows = []
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
    def __init__(self, stream):
        self._stream = stream

    def from_stub(self, name, version):
        stream = Stream(self._stream, name, version)
        return Dataset(stream)

    def from_list(datalist):
        stream = LocalStream(datalist)
        return Dataset(stream)

    def repeat(self, num):
        if num <= 0:
            raise Exception("Must repeat 1 or more times")
        self._stream = Repeat(num, self._stream)
        return self

    def shuffle(self, buffer_size):
        if buffer_size <= 0:
            raise Exception("Buffer size must be greater than or equal to 1")
        self._stream = Shuffle(buffer_size, self._stream)
        return self

    def batch(self, batch_size):
        if batch_size <= 0:
            raise Exception("Batch size must be greater than or equal to 1")
        self._stream = Batch(batch_size, self._stream)
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
        return self._row[:-1]

    def label(self):
        return self._label

    def to_numpy(self):
        return self._row()

    def __repr__(self):
        return "Features: {} , Label: {}".format(self.features(), self.label())


class LocalRow:

    def __init__(self, row_list):
        self._features = row_list[:-1]
        self._row = row_list
        self._label = row_list[-1]

    def features(self):
        return self._features

    def label(self):
        return self._label

    def to_numpy(self):
        return np.array(self._row)

    def __repr__(self):
        return "Features: {} , Label: {}".format(self.features(), self.label())


def parse_proto_value(value):
    """ parse_proto_value is used to parse the one of Value message
	"""
    return getattr(value, value.WhichOneof("value"))

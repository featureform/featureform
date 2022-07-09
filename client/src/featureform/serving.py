# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
import json
import marshal

import grpc
import numpy as np
from .proto import serving_pb2
from .proto import serving_pb2_grpc
import random
import os
from .sqlite_metadata import SQLiteMetadata

import pandas as pd
import types


class LocalClient:
    def __init__(self):
        self.sqldb = SQLiteMetadata()

    def process_transformation(self, name, variant):
        source_row = self.sqldb.getNameVariant("source_variant", "name", name, "variant", variant)[0]
        inputs = json.loads(source_row[9])
        dataframes = []
        code = marshal.loads(bytearray(source_row[10]))
        func = types.FunctionType(code, globals(), "transformation")
        for input in inputs:
            source_name, source_variant = input[0], input[1],
            if self.sqldb.is_transformation(source_name, source_variant):
                df = self.process_transformation(source_name, source_variant)
                dataframes.append(df)
            else:
                source_row = \
                    self.sqldb.getNameVariant("source_variant", "name", source_name, "variant", source_variant)[0]
                df = pd.read_csv(str(source_row[10]))
                dataframes.append(df)
        new_data = func(*dataframes)
        return new_data

    def features(self, feature_variant_list, entity_tuple):
        feature_dataframes = set()
        dataframe_mapping = {}
        for featureVariantTuple in feature_variant_list:

            feature_row = self.sqldb.getNameVariant("feature_variant", "featureName", featureVariantTuple[0],
                                                    "variantName", featureVariantTuple[1])[0]
            feature_column_name, source_name, source_variant = feature_row[11], feature_row[12], feature_row[13]
            source_row = self.sqldb.getNameVariant("source_variant", "name", source_name, "variant", source_variant)[0]
            if self.sqldb.is_transformation(source_name, source_variant):
                df = self.process_transformation(source_name, source_variant)
                if isinstance(df, pd.Series):
                    df = df.to_frame()
                #if feature_column_name not in df.columns:
                    df.reset_index(inplace=True)
                df = df[[entity_tuple[0], feature_column_name]]
                df.set_index(entity_tuple[0])

                feature_dataframes.add(featureVariantTuple[0])
                dataframe_mapping[featureVariantTuple[0]] = df
            else:
                feature_dataframes, dataframe_mapping = self.process_feature_csv(source_row[10], entity_tuple[0],
                                                                                 feature_dataframes,
                                                                                 feature_column_name,
                                                                                 dataframe_mapping,
                                                                                 featureVariantTuple[0], "")
        try:
            all_feature_df = dataframe_mapping[feature_dataframes.pop()]
            while len(feature_dataframes) != 0:
                featureDF = dataframe_mapping[feature_dataframes.pop()]
                all_feature_df = all_feature_df.join(featureDF.set_index(entity_tuple[0]), on=entity_tuple[0], lsuffix='_left')
        except TypeError:
            print("Set is empty")
        entity_row = all_feature_df.loc[all_feature_df[entity_tuple[0]] == entity_tuple[1]]
        return entity_row

    def process_feature_csv(self, source_path, entity_name, feature_dataframes, feature_column_name, dataframe_mapping,
                            featureName, timestamp_column):
        df = pd.read_csv(str(source_path))
        if timestamp_column != "":
            df = df[[entity_name, feature_column_name, timestamp_column]]
        else:
            df = df[[entity_name, feature_column_name]]

        df.set_index(entity_name)
        feature_dataframes.add(featureName)

        dataframe_mapping[featureName] = df.sort_values(by=feature_column_name, ascending=False)
        return feature_dataframes, dataframe_mapping

    def processLabelCSV(self, source_path, entity_name, labelColumnName, timestamp_column):
        df = pd.read_csv(source_path)
        if timestamp_column != "":
            df = df[[entity_name, labelColumnName, timestamp_column]]
        else:
            df = df[[entity_name, labelColumnName]]
        df.set_index(entity_name)
        return df

    def training_set(self, trainingSetName, trainingSetVariant):
        feature_dataframes = set()
        dataframe_mapping = {}
        trainingSetRow = \
            self.sqldb.getNameVariant("training_set_variant", "trainingSetName", trainingSetName, "variantName",
                                      trainingSetVariant)[0]
        labelRow = \
            self.sqldb.getNameVariant("labels_variant", "labelName", trainingSetRow[5], "variantName",
                                      trainingSetRow[6])[0]
        labelSource = self.sqldb.getNameVariant("source_variant", "name", labelRow[12], "variant", labelRow[13])[0]
        if self.sqldb.is_transformation(labelRow[12], labelRow[13]):
            df = self.process_transformation(labelRow[12], labelRow[13])
            if labelRow[9] != "":
                df = df[[labelRow[8], labelRow[10], labelRow[9]]]
            else:
                df = df[[labelRow[8], labelRow[10]]]
            df.set_index(labelRow[8])
            labelDF = df
        else:
            labelDF = self.processLabelCSV(labelSource[10], labelRow[8], labelRow[10], labelRow[9])

        featureTable = self.sqldb.getNameVariant("training_set_features", "trainingSetName", trainingSetName,
                                                 "trainingSetVariant", trainingSetVariant)

        labelDF.rename(columns={labelRow[10]: 'label'}, inplace=True)
        trainingset_df = labelDF
        for featureVariant in featureTable:
            feature_row = self.sqldb.getNameVariant("feature_variant", "featureName", featureVariant[2], "variantName",
                                                    featureVariant[3])[0]

            source_row = \
                self.sqldb.getNameVariant("source_variant", "name", feature_row[12], "variant", feature_row[13])[0]

            name_variant = featureVariant[2] + "." + featureVariant[3]
            if self.sqldb.is_transformation(feature_row[12], feature_row[13]):
                df = self.process_transformation(feature_row[12], feature_row[13])
                if isinstance(df, pd.Series):
                    df = df.to_frame()
               # if feature_row[11] not in df.columns:
                    df.reset_index(inplace=True)
                if feature_row[10] != "":
                    df = df[[feature_row[9], feature_row[11], feature_row[10]]]
                else:
                    df = df[[feature_row[9], feature_row[11]]]

                df.set_index(feature_row[9])

                df.rename(columns={feature_row[11]: name_variant}, inplace=True)
                feature_df = df#.sort_values(by=feature_row[10], ascending=False)
            else:
                df = pd.read_csv(str(source_row[10]))
                if featureVariant[2] != "":
                    df = df[[feature_row[9], feature_row[11], feature_row[10]]]
                else:
                    df = df[[feature_row[9], feature_row[11]]]
                df.set_index(feature_row[9])
                df.rename(columns={feature_row[11]: name_variant}, inplace=True)
                feature_df = df
            if feature_row[10] != "":
                trainingset_df = pd.merge_asof(trainingset_df, feature_df.sort_values(['ts']), direction='backward',
                                           left_on=labelRow[9], right_on=feature_row[10], left_by=labelRow[8],
                                           right_by=feature_row[9])
            else:
                feature_df.drop_duplicates(subset=[feature_row[9], name_variant])
                trainingset_df[labelRow[8]]=trainingset_df[labelRow[8]].astype('string')
                feature_df[labelRow[8]]=feature_df[labelRow[8]].astype('string')
                trainingset_df = trainingset_df.join(feature_df.set_index(labelRow[8]), how="left", on=labelRow[8], lsuffix="_left")
                # trainingset_df = pd.merge_asof(trainingset_df, feature_df.sort_values(feature_row[9]), direction='backward',
                #                                left_by=labelRow[8],
                #                                right_by=feature_row[9])

        if labelRow[9] != "":
            trainingset_df.drop(columns=labelRow[9], inplace=True)
        trainingset_df.drop(columns=labelRow[8], inplace=True)
        label_col = trainingset_df.pop('label')
        trainingset_df = trainingset_df.assign(label=label_col)
        return Dataset.from_list(trainingset_df.values.tolist())


class Client:

    def __init__(self, host, tls_verify=True, cert_path=None):
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

    def dataset(self, name, version):
        return Dataset(self._stub).from_stub(name, version)

    def features(self, features, entities):
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
        self._iter = self._datalist


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

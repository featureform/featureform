# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

import re
import grpc
import numpy as np
from .proto import serving_pb2
from .proto import serving_pb2_grpc
import random
import os
from .sqlite_metadata import SQLiteMetadata
import csv
import pandas as pd

class LocalClient:
    def __init__(self):
        self.sqldb = SQLiteMetadata()

    def features(self, featureVariantList, entityTuple):
        feature_dataframes = set()
        dataframeMapping = {}
        for featureVariantTuple in featureVariantList:
            featureRow = self.sqldb.getNameVariant("feature_variant",  "featureName", featureVariantTuple[0], "variantName", featureVariantTuple[1])[0]
            featureColumnName, sourceName, sourceVariant = featureRow[11], featureRow[12], featureRow[13]
            sourceRow = self.sqldb.getNameVariant("source_variant",  "sourceName", sourceName, "variant", sourceVariant)[0]
            feature_dataframes, dataframeMapping = self.processFeatureCSV(sourceRow[8], entityTuple[0], feature_dataframes, featureColumnName, dataframeMapping, featureVariantTuple[0])
        try:
            allFeatureDF = dataframeMapping[feature_dataframes.pop()]
            while len(feature_dataframes) != 0:
                featureDF = dataframeMapping[feature_dataframes.pop()]
                allFeatureDF = allFeatureDF.join(featureDF.set_index(entityTuple[0]), on=entityTuple[0])
        except TypeError:
            print("Set is empty")
        entityRow = allFeatureDF.loc[allFeatureDF[entityTuple[0]] == entityTuple[1]]
        return entityRow

    def processFeatureCSV(self, sourcePath, entityName, featureDataframes, featureColumnName, dataframeMapping, featureName):
        df = pd.read_csv(sourcePath)
        df = df[[entityName, featureColumnName]]
        df.set_index(entityName)
        featureDataframes.add(featureName)
        dataframeMapping[featureName] = df
        return featureDataframes, dataframeMapping

    def processLabelCSV(self, sourcePath, entityName, labelColumnName):
        df = pd.read_csv(sourcePath)
        df = df[[entityName, labelColumnName]]
        df.set_index(entityName)
        return df

    def training_set(self, trainingSetName, trainingSetVariant):
        featureDataframes = set()
        dataframeMapping = {}
        trainingSetRow = self.sqldb.getNameVariant("training_set_variant",  "trainingSetName", trainingSetName, "variantName", trainingSetVariant)[0]
        labelRow = self.sqldb.getNameVariant("labels_variant",  "labelName", trainingSetRow[5], "variantName", trainingSetRow[6])[0]
        labelSource = self.sqldb.getNameVariant("source_variant",  "sourceName", labelRow[12], "variant", labelRow[13])[0]
        labelDF = self.processLabelCSV(labelSource[8], labelRow[8], labelRow[10])
        featureTable = self.sqldb.getNameVariant("training_set_features", "trainingSetName", trainingSetName, "trainingSetVariant", trainingSetVariant)
        for featureVariant in featureTable:
            featureRow = self.sqldb.getNameVariant("feature_variant",  "featureName", featureVariant[2], "variantName", featureVariant[3])[0]
            sourceRow = self.sqldb.getNameVariant("source_variant",  "sourceName", featureRow[12], "variant", featureRow[13])[0]
            feature_dataframes, dataframeMapping = self.processFeatureCSV(sourceRow[8], featureRow[9], featureDataframes, featureRow[11], dataframeMapping, featureVariant[2])
        try:
            allFeatureDF = dataframeMapping[feature_dataframes.pop()]
            while len(feature_dataframes) != 0:
                featureDF = dataframeMapping[feature_dataframes.pop()]
                allFeatureDF = allFeatureDF.join(featureDF.set_index(featureRow[9]), on=featureRow[9])
        except TypeError:
            print("Set is empty")
        allFeatureDF = allFeatureDF.join(labelDF.set_index(featureRow[9]), on=featureRow[9])
        allFeatureDF.drop(columns=featureRow[9])
        return Dataset.from_list(allFeatureDF.values.tolist())

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
        return Dataset(self._stub, name, version)

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
        self._iter = datalist

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

    def from_stub(stub, name, version):
        stream = Stream(stub, name, version)
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

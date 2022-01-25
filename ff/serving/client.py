import grpc
import numpy as np
import proto.serving_pb2
import proto.serving_pb2_grpc
import random


class Client:

    def __init__(self, host):
        channel = grpc.insecure_channel(host,
                                        options=(('grpc.enable_http_proxy',
                                                  0),))
        self._stub = proto.serving_pb2_grpc.FeatureStub(channel)

    def dataset(self, name, version):
        return Dataset(self._stub, name, version)

    def features(self, features, entities):
        req = proto.serving_pb2.FeatureServeRequest()
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


class Dataset:

    def __init__(self, stub, name, version):
        req = proto.serving_pb2.TrainingDataRequest()
        req.id.name = name
        req.id.version = version
        self.name = name
        self.version = version
        self._stub = stub
        self._req = req
        self._iter = stub.TrainingData(req)
        self._batch_size = 3
        self._shuffle_batch = False
        self._shuffled_data = []

    def batch(self, num):
        if num <= 0:
            raise ValueError("Batch must be a positive integer")
        self._batch_size = num
        return self

    def unbatch(self):
        self._batch_size = 0
        return self

    def repeat(self):
    	'''
    	Repeat the dataset
    	'''
    	self._iter = self._stub.TrainingData(self._req)

    def shuffle_batch(self, shuffle_batch):
    	'''
    	Shuffle data in each batch
    	'''
    	self._shuffle_batch = shuffle_batch

    def shuffle(self):
    	'''
    	Shuffle the dataset
    	'''
    	sub_shuffled_data = []
    	try:
	    	for i in range(1000):
	    		sub_shuffled_data.append(Row(next(self._iter)))
    	except StopIteration:
	    	if len(sub_shuffled_data) == 0:
	    		raise

    	random.shuffle(sub_shuffled_data)
    	self._shuffled_data.extend(sub_shuffled_data)


    def __iter__(self):
        return self

    def __next__(self):
        if self._batch_size == 0:
        	if len(self._shuffled_data) == 0:
        		self.shuffle()
        	return self._shuffled_data.pop(0)

        rows = []
        for i in range(self._batch_size):
        	if len(self._shuffled_data) == 0:
        		self.shuffle()
        	rows.append(self._shuffled_data.pop(0))
        if self._shuffle_batch:
        	random.shuffle(rows)
        return rows
    	

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

    def to_numpy():
        return self._row()

    def __repr__(self):
        return "Features: {} , Label: {}".format(self.features(), self.label())


def parse_proto_value(value):
    """ parse_proto_value is used to parse the one of Value message
    """
    return getattr(value, value.WhichOneof("value"))


client = Client("localhost:8080")
dataset = client.dataset("f1", "v1")
print([r for r in dataset])
dataset.repeat()
print(client.features([("f1", "v1")], {"user": "a"}))

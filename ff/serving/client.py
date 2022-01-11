import grpc
import numpy as np
import proto.serving_pb2
import proto.serving_pb2_grpc


class Client:

    def __init__(self, host):
        channel = grpc.insecure_channel(host,
                                        options=(('grpc.enable_http_proxy',
                                                  0),))
        self._stub = proto.serving_pb2_grpc.FeatureStub(channel)

    def dataset(self, name, version):
        return Dataset(self._stub, name, version)


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
        self._batch_size = 0

    def batch(self, num):
        if num <= 0:
            raise ValueError("Batch must be a positive integer")
        self._batch_size = num
        return self

    def unbatch(self):
        self._batch_size = 0
        return self

    def __iter__(self):
        return self

    def __next__(self):
        if self._batch_size == 0:
            return Row(next(self._iter))
        rows = []
        try:
            for i in range(self._batch_size):
                rows.append(Row(next(self._iter)))
            return rows
        except StopIteration:
            if len(rows) == 0:
                raise
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
    """ parse_proto_value is used to parse the oneof Value message
    """
    return getattr(value, value.WhichOneof("value"))


client = Client("localhost:8080")
dataset = client.dataset("f1", "v1")
print([r for r in dataset])

from collections import deque
from typing import Any, List, Tuple, Union

import grpc
import numpy as np
from dataclasses import dataclass

from featureform import Model
from featureform.proto import serving_pb2


@dataclass
class TrainTestSplitDetails:
    name: str
    version: str
    model: Union[str, Model]
    test_size: float
    shuffle: bool
    random_state: int
    batch_size: int = 1

    def to_proto(
        self, request_type: serving_pb2.RequestType
    ) -> serving_pb2.TrainTestSplitRequest:
        req = serving_pb2.TrainTestSplitRequest()
        req.id.name = self.name
        req.id.version = self.version
        if self.model is not None:
            req.model.name = (
                self.model if isinstance(self.model, str) else self.model.name
            )
        req.test_size = self.test_size
        req.shuffle = self.shuffle
        req.random_state = self.random_state
        req.request_type = request_type
        req.batch_size = self.batch_size
        return req


class _SplitStream:
    """
    Handles the gRPC stream for the train test split
    """

    def __init__(self, stub, train_set_split_details: TrainTestSplitDetails):
        self.request_queue = deque()
        self.response_stream = None
        self.stub = stub
        self.train_test_split_details = train_set_split_details

    def start(self):
        """Initializes the gRPC stream."""
        self.response_stream = self.stub.TrainTestSplit(
            self._create_request_generator()
        )
        init_response = self.send_request(serving_pb2.RequestType.INITIALIZE)
        if not init_response.initialized:
            raise ValueError("Failed to initialize train test split")
        return self

    def send_request(self, request_type) -> serving_pb2.BatchTrainTestSplitResponse:
        """Sends a request through the stream and returns the response."""
        req = self.train_test_split_details.to_proto(request_type)
        self.request_queue.append(req)
        try:
            response = next(self.response_stream)
            return response
        except grpc.RpcError as e:
            print(f"Caught RPC error of type: {e.code()} - {e.details()}")
            if e.code() == grpc.StatusCode.RESOURCE_EXHAUSTED:
                print("Message size exceeded. Try reducing the batch size.")
            raise

    def _create_request_generator(self):
        """Yields requests from the queue for the gRPC stream."""
        while True:
            if self.request_queue:
                yield self.request_queue.popleft()


@dataclass
class _NpProtoTypeParser:
    """Parses the proto types to numpy types"""

    nptype: np.dtype
    feature_proto_types: List[Tuple[str, np.dtype]]
    label_proto_type: str

    @staticmethod
    def init_types(row: serving_pb2.TrainingDataRow):
        from featureform.serving import (
            get_numpy_array_type,
            proto_type_to_np_type,
        )

        types = [
            (feature.WhichOneof("value"), proto_type_to_np_type(feature))
            for feature in row.features
        ]
        numpy_array_type = get_numpy_array_type(list(map(lambda x: x[1], types)))
        label_proto_type = row.label.WhichOneof("value")
        return _NpProtoTypeParser(numpy_array_type, types, label_proto_type)

    def parse_features(self, rows: List[serving_pb2.Value]) -> List[Any]:
        return [
            getattr(feature, self.feature_proto_types[i][0])
            for i, feature in enumerate(rows)
        ]

    def parse_label(self, row: serving_pb2.Value) -> Any:
        return getattr(row, self.label_proto_type)


class TrainTestSplitIterator:
    def __init__(
        self,
        split_stream: _SplitStream,
        batch_size: int,
        request_type: serving_pb2.RequestType,
    ):
        self.request_type = request_type
        self._split_stream = split_stream
        self._batch_size = batch_size
        self._complete = False
        self._np_type_parser = None

    @staticmethod
    def train_iter(split_stream, batch_size):
        return TrainTestSplitIterator(
            split_stream, batch_size, serving_pb2.RequestType.TRAINING
        )

    @staticmethod
    def test_iter(split_stream, batch_size):
        return TrainTestSplitIterator(
            split_stream, batch_size, serving_pb2.RequestType.TEST
        )

    def __iter__(self):
        return self

    def __next__(self) -> Tuple[np.ndarray, np.ndarray]:
        if self._complete:
            raise StopIteration

        batch_features_list = []
        batch_labels_list = []

        result = self._split_stream.send_request(self.request_type)
        data = result.data

        if self._np_type_parser is None and data.rows:
            self._np_type_parser = _NpProtoTypeParser.init_types(data.rows[0])

        for row in data.rows:
            batch_features_list.append(
                self._np_type_parser.parse_features(row.features)
            )
            batch_labels_list.append(self._np_type_parser.parse_label(row.label))

        if result.iterator_done:
            self._complete = True

        if not batch_features_list:  # If no data was collected
            raise StopIteration

        batch_features = np.array(
            batch_features_list, dtype=self._np_type_parser.nptype
        )
        batch_labels = np.array(batch_labels_list)

        return batch_features, batch_labels


class TrainTestSplit:
    """
    Returns two iterators, one for the training set and one for the test set, in that order
    """

    def __init__(
        self,
        stub,
        name: str,
        version: str,
        test_size: float,
        train_size: float,  # unused for now
        shuffle: bool,
        random_state: int,
        batch_size: int,
        model: Union[str, Model] = None,
    ):
        self.batch_size = batch_size
        self.train_iter = None
        self.test_iter = None

        self.train_test_split_details = TrainTestSplitDetails(
            name=name,
            version=version,
            model=model,
            test_size=test_size,
            shuffle=shuffle,
            random_state=random_state,
            batch_size=batch_size,
        )

        self._split_stream = _SplitStream(stub, self.train_test_split_details).start()

    def split(self):
        return (
            TrainTestSplitIterator.train_iter(self._split_stream, self.batch_size),
            TrainTestSplitIterator.test_iter(self._split_stream, self.batch_size),
        )

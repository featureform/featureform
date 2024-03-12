import datetime
from typing import Tuple, Union

import grpc
import numpy as np
from dataclasses import dataclass

from featureform import Model
from featureform.proto import serving_pb2


@dataclass
class TrainingSetSplitDetails:
    name: str
    version: str
    model: Union[str, Model]
    test_size: float
    shuffle: bool
    random_state: int
    batch_size: int = 1

    def to_proto(
        self, request_type: serving_pb2.RequestType
    ) -> serving_pb2.TrainingTestSplitRequest:
        req = serving_pb2.TrainingTestSplitRequest()
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
    Handles the gRPC stream for the training test split
    """

    def __init__(self, stub, training_set_split_details: TrainingSetSplitDetails):
        self.request_queue = []
        self.response_stream = None
        self.stub = stub
        self.training_set_split_details = training_set_split_details

    def start(self):
        self.response_stream = self.stub.TrainingTestSplit(
            self._create_request_generator()
        )

        # verify initialization response
        init_response = self.send_request(serving_pb2.RequestType.INITIALIZE)
        if not init_response.initialized:
            raise ValueError("Failed to initialize training test split")

        return self

    def send_request(self, request_type) -> serving_pb2.BatchTrainTestSplitResponse:
        req = self.training_set_split_details.to_proto(request_type)
        self.request_queue.append(req)
        # TODO: Attempting to add a better error here if the message size is too large rather than throwing the GRPC stack
        try:
            response = next(self.response_stream)
            return response
        except grpc.RpcError as e:
            # Check if the exception is due to resource exhaustion
            if e.code() == grpc.StatusCode.RESOURCE_EXHAUSTED:
                print("Caught RESOURCE_EXHAUSTED error")
                return response
            else:
                # Handle other types of exceptions
                print(f"Caught RPC error of type: {e.code()} - {e.details()}")
                return response

    def _create_request_generator(self):
        """
        Creates a generator that yields requests from the request queue. Used to pass to the gRPC stub.
        """
        while True:
            if len(self.request_queue) > 0:
                request = self.request_queue.pop()
                yield request


class TrainingSetSplitIterator:
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

    @staticmethod
    def train_iter(split_stream, batch_size):
        return TrainingSetSplitIterator(
            split_stream, batch_size, serving_pb2.RequestType.TRAINING
        )

    @staticmethod
    def test_iter(split_stream, batch_size):
        return TrainingSetSplitIterator(
            split_stream, batch_size, serving_pb2.RequestType.TEST
        )

    def __iter__(self):
        return self

    def __next__(self) -> Tuple[np.ndarray, np.ndarray]:
        global types
        if self._complete:
            raise StopIteration

        batch_features_list = []
        batch_labels_list = []

        start = datetime.datetime.now()
        next_row = self._split_stream.send_request(self.request_type)
        end = datetime.datetime.now()
        print("Request time: ", end - start)
        rows = next_row.rows

        # Process and store the row data
        from featureform.serving import (
            parse_proto_value,
            get_numpy_array_type,
            proto_type_to_np_type,
        )

        start = datetime.datetime.now()
        for i, row in enumerate(rows.rows):
            if i == 0:
                types = [proto_type_to_np_type(feature) for feature in row.features]
                nptype = get_numpy_array_type(types)
                value_types = [feature.WhichOneof("value") for feature in row.features]

            features = [
                getattr(feature, value_types[j])
                for j, feature in enumerate(row.features)
            ]

            label = parse_proto_value(row.label)

            # for row in np_rows:
            batch_features_list.append(features)
            batch_labels_list.append(label)
        end = datetime.datetime.now()
        print("Parse time: ", end - start)

        if next_row.iterator_done:
            self._complete = True

        if not batch_features_list:  # If no data was collected
            raise StopIteration

        batch_features = np.array(batch_features_list, dtype=nptype)
        batch_labels = np.array(batch_labels_list)

        return batch_features, batch_labels


class TrainingSetTestSplit:
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

        self.training_set_split_details = TrainingSetSplitDetails(
            name=name,
            version=version,
            model=model,
            test_size=test_size,
            shuffle=shuffle,
            random_state=random_state,
            batch_size=batch_size,
        )

        self._split_stream = _SplitStream(stub, self.training_set_split_details).start()

    def split(self):
        return (
            TrainingSetSplitIterator.train_iter(self._split_stream, self.batch_size),
            TrainingSetSplitIterator.test_iter(self._split_stream, self.batch_size),
        )

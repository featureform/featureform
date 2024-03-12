import os

import numpy as np
import pytest

from featureform.proto import serving_pb2
from featureform.serving import Dataset

real_path = os.path.realpath(__file__)
dir_path = os.path.dirname(real_path)


def response(
    req_type, iterator_done, batch_size
) -> serving_pb2.BatchTrainTestSplitResponse:
    if req_type == serving_pb2.RequestType.INITIALIZE:
        return serving_pb2.BatchTrainTestSplitResponse(
            request_type=req_type, initialized=True
        )
    elif req_type == serving_pb2.RequestType.TRAINING:
        label_value = "train"
    elif req_type == serving_pb2.RequestType.TEST:
        label_value = "test"

    row = serving_pb2.TrainingDataRow(
        features=[
            serving_pb2.Value(str_value="f1"),
            serving_pb2.Value(int_value=1),
            serving_pb2.Value(bool_value=False),
        ],
        label=serving_pb2.Value(str_value=label_value),
    )

    return serving_pb2.BatchTrainTestSplitResponse(
        request_type=req_type,
        data=serving_pb2.TrainingDataRows(rows=[row] * batch_size),
        iterator_done=iterator_done,
    )


class MockGrpcStub:
    def __init__(self, num_rows, batch_size=1):
        self.num_rows = num_rows
        self.train_rows = 0
        self.test_rows = 0
        self.batch_size = batch_size

    def TrainTestSplit(self, iterator) -> serving_pb2.BatchTrainTestSplitResponse:
        rows_to_return = self.batch_size
        for value in iterator:
            iterator_done = False
            if value.request_type == serving_pb2.RequestType.TRAINING:
                self.train_rows += self.batch_size

                if self.train_rows > self.num_rows:
                    rows_to_return = self.num_rows - (self.train_rows - self.batch_size)
                    iterator_done = True
            elif value.request_type == serving_pb2.RequestType.TEST:
                self.test_rows += self.batch_size

                if self.test_rows > self.num_rows:
                    rows_to_return = self.num_rows - (self.test_rows - self.batch_size)
                    iterator_done = True
            yield response(value.request_type, iterator_done, rows_to_return)


class MockStream:
    name = None
    version = None
    model = None
    _stub = None

    def __init__(self, name, version, num_rows, batch_size=1):
        self.name = name
        self.version = version
        self._stub = MockGrpcStub(num_rows, batch_size)


@pytest.mark.parametrize(
    "kwargs, should_fail",
    [
        (
            {
                "test_size": 0.5,
                "train_size": 0.5,
                "shuffle": True,
                "random_state": None,
            },
            False,
        ),
        (
            {
                "test_size": -0.5,
                "train_size": 0.5,
                "shuffle": True,
                "random_state": None,
            },
            True,
        ),
        (
            {
                "test_size": 0.5,
                "train_size": -0.5,
                "shuffle": True,
                "random_state": None,
            },
            True,
        ),
        ({"test_size": 0.5, "shuffle": True, "random_state": None}, False),
        ({"test_size": 0.5, "random_state": None}, False),
        ({"test_size": 0.5}, False),
        ({"test_size": 0.5, "random_state": 3}, False),
        ({"train_size": 0.5, "shuffle": True, "random_state": None}, False),
        (
            {
                "test_size": 0.5,
                "train_size": 0.1,
                "shuffle": True,
                "random_state": None,
            },
            True,
        ),
    ],
)
def test_initialization(kwargs, should_fail):
    train_test_stream = MockStream("name", "variant", 1)
    dataset = Dataset(train_test_stream)
    if should_fail:
        with pytest.raises(Exception):
            dataset.train_test_split(**kwargs)
    else:
        dataset.train_test_split(**kwargs)


def test_train_test_split():
    train_test_stream = MockStream("name", "variant", 1)
    dataset = Dataset(train_test_stream)
    train, test = dataset.train_test_split(
        test_size=0.5, train_size=0.5, shuffle=True, random_state=None
    )

    for features, label in train:
        assert np.array_equal(features, np.array([["f1", 1, False]], dtype="O"))
        assert np.array_equal(label, np.array(["train"]))

    for features, label in test:
        assert np.array_equal(features, np.array([["f1", 1, False]], dtype="O"))
        assert np.array_equal(label, np.array(["test"]))


def test_train_test_batch():
    train_test_stream = MockStream("name", "variant", 10, batch_size=5)
    dataset = Dataset(train_test_stream)
    train, test = dataset.train_test_split(
        test_size=0.5, train_size=0.5, shuffle=True, random_state=None, batch_size=5
    )
    i = 0
    for features, label in train:
        if i > 1:
            break  # Ensures we only check the first two batches

        assert np.array_equal(
            features,
            np.array(
                [
                    ["f1", 1, False],
                    ["f1", 1, False],
                    ["f1", 1, False],
                    ["f1", 1, False],
                    ["f1", 1, False],
                ],
                dtype="O",
            ),
        )
        assert np.array_equal(
            label, np.array(["train", "train", "train", "train", "train"])
        )
        i += 1

    i = 0
    for features, label in test:
        if i > 1:
            break
        assert np.array_equal(
            features,
            np.array(
                [
                    ["f1", 1, False],
                    ["f1", 1, False],
                    ["f1", 1, False],
                    ["f1", 1, False],
                    ["f1", 1, False],
                ],
                dtype="O",
            ),
        )
        assert np.array_equal(label, np.array(["test", "test", "test", "test", "test"]))
        i += 1


def test_train_test_partial_batch():
    train_test_stream = MockStream("name", "variant", 3, batch_size=5)
    dataset = Dataset(train_test_stream)
    train, test = dataset.train_test_split(
        test_size=0.5, train_size=0.5, shuffle=True, random_state=None, batch_size=5
    )
    i = 0
    for features, label in train:
        if i > 1:
            break  # Ensures we only check the first two batches

        assert np.array_equal(
            features,
            np.array(
                [
                    ["f1", 1, False],
                    ["f1", 1, False],
                    ["f1", 1, False],
                ],
                dtype="O",
            ),
        )
        assert np.array_equal(label, np.array(["train", "train", "train"]))
        i += 1

    i = 0
    for features, label in test:
        if i > 1:
            break
        assert np.array_equal(
            features,
            np.array(
                [
                    ["f1", 1, False],
                    ["f1", 1, False],
                    ["f1", 1, False],
                ],
                dtype="O",
            ),
        )
        assert np.array_equal(label, np.array(["test", "test", "test"]))
        i += 1

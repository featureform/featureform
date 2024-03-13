import featureform as ff
from featureform.serving import Dataset
import os
from featureform.proto import serving_pb2
import numpy as np
import pytest
import time

real_path = os.path.realpath(__file__)
dir_path = os.path.dirname(real_path)


def response(req_type, iterator_done):
    if req_type == 0:
        request_type = serving_pb2.RequestType.INITIALIZE
        return serving_pb2.TrainingTestSplitResponse(
            request_type=request_type, initialized=True
        )
    elif req_type == 1:
        request_type = serving_pb2.RequestType.TRAINING
        label_value = "train"
    elif req_type == 2:
        request_type = serving_pb2.RequestType.TEST
        label_value = "test"

    req = serving_pb2.TrainingTestSplitResponse(
        request_type=request_type,
        row=serving_pb2.TrainingDataRow(
            features=[
                serving_pb2.Value(str_value="f1"),
                serving_pb2.Value(int_value=1),
                serving_pb2.Value(bool_value=False),
            ],
            label=serving_pb2.Value(str_value=label_value),
        ),
        iterator_done=iterator_done,
    )

    # training_data = serving_pb2.TrainingDataRow()
    # req.row = training_data

    return req


class MockGrpcStub:
    def __init__(self, num_rows):
        self.num_rows = num_rows
        self.train_rows = 0
        self.test_rows = 0

    def TrainingTestSplit(self, iterator):
        for value in iterator:
            iterator_done = False
            if value.request_type == 1:
                self.train_rows += 1

                if self.train_rows >= self.num_rows:
                    iterator_done = True
            elif value.request_type == 2:
                self.test_rows += 1

                if self.test_rows >= self.num_rows:
                    iterator_done = True
            yield response(value.request_type, iterator_done)


class MockStream:
    name = None
    version = None
    model = None
    _stub = None

    def __init__(self, name, version, num_rows):
        self.name = name
        self.version = version
        self.num_rows = num_rows
        self._stub = MockGrpcStub(self.num_rows)


@pytest.mark.parametrize(
    "kwargs,should_fail",
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
    train_test_stream = MockStream("name", "variant", 10)
    dataset = Dataset(train_test_stream)
    train, test = dataset.train_test_split(
        test_size=0.5, train_size=0.5, shuffle=True, random_state=None, batch_size=5
    )
    i = 0
    for features, label in train:
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
    train_test_stream = MockStream("name", "variant", 3)
    dataset = Dataset(train_test_stream)
    train, test = dataset.train_test_split(
        test_size=0.5, train_size=0.5, shuffle=True, random_state=None, batch_size=5
    )
    i = 0
    for features, label in train:
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

import featureform as ff
from featureform.serving import Dataset
import os
from featureform.proto import serving_pb2
import numpy as np
import pytest
import time

real_path = os.path.realpath(__file__)
dir_path = os.path.dirname(real_path)

def response(req_type):
    if req_type == 1:
        request_type = serving_pb2.RequestType.TEST
        label_value = "test"
    elif req_type == 2:
        request_type = serving_pb2.RequestType.TRAINING
        label_value = "train"
    req = serving_pb2.GetTrainingTestSplitResponse(
        request_type=request_type,
        row=serving_pb2.TrainingDataRow(
            features=[
                serving_pb2.Value(
                    str_value="f1"
                ),
                serving_pb2.Value(
                    str_value="f2"
                ),
                serving_pb2.Value(
                    str_value="f3"
                )
            ],
            label=serving_pb2.Value(
                str_value=label_value
            )
        )
    )


    # training_data = serving_pb2.TrainingDataRow()
    # req.row = training_data
    return req


class MockStub:
    def __init__(self):
        pass

    def GetTrainingTestSplit(self, iterator):
        for value in iterator:
            yield response(value.request_type)


class MockStream:
    name = None
    version = None
    model = None
    _stub = MockStub()

    def __init__(self, name, version):
        self.name = name
        self.version = version


@pytest.mark.parametrize(
    "kwargs,should_fail",
    [
        ({"test_size": 0.5, "train_size": 0.5, "shuffle": True, "random_state": None}, False),
        ({"test_size": -0.5, "train_size": 0.5, "shuffle": True, "random_state": None}, True),
        ({"test_size": 0.5, "train_size": -0.5, "shuffle": True, "random_state": None}, True),
        ({"test_size": 0.5, "shuffle": True, "random_state": None}, False),
        ({"test_size": 0.5, "random_state": None}, False),
        ({"test_size": 0.5}, False),
        ({"test_size": 0.5, "random_state": 3}, False),
        ({"train_size": 0.5, "shuffle": True, "random_state": None}, False),
        ({"test_size": 0.5, "train_size": 0.1, "shuffle": True, "random_state": None}, True)
     ]

)
def test_initialization(kwargs, should_fail):
    train_test_stream = MockStream("name", "variant")
    dataset = Dataset(train_test_stream)
    if should_fail:
        with pytest.raises(Exception):
            dataset.train_test_split(**kwargs)
    else:
        dataset.train_test_split(**kwargs)

def test_train_test_split():
    train_test_stream = MockStream("name", "variant")
    dataset = Dataset(train_test_stream)
    train, test = dataset.train_test_split(test_size=0.5, train_size=.5, shuffle=True, random_state=None)

    for i, row in enumerate(train):
        if i > 1:
            break
        assert np.array_equal(row.features(), np.array(["f1", "f2", "f3"]))
        assert row.label() == "train"
    for i, row in enumerate(test):
        if i > 1:
            break
        assert np.array_equal(row.features(), np.array(["f1", "f2", "f3"]))
        assert row.label() == "test"

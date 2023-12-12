from unittest.mock import MagicMock
import pytest
import featureform as ff
import pandas as pd
from featureform.register import ColumnSourceRegistrar
from featureform.register import Registrar
from featureform.resources import SourceVariant

# Mock DataFrameStats for testing
mock_stats_df = pd.DataFrame({"a": [1, 4], "b": [2, 5], "c": [3, 6]})

@pytest.fixture()
def client():
    return ff.Client(insecure=True, local=True)


@pytest.fixture()
def mock_proto():
    return pd.DataFrame({"a": [1, 4], "b": [2, 5], "c": [3, 6]})

def create_datatset(mock_proto):
    ColumnSourceRegistrar()
    return pd.DataFrame(mock_proto)

# conftest.py
# 1. data_dictionary() (fixture) -> {columns: [""], data: [[],[]]}
# 2. features_dataframe(data_dictionary) (fixture) -> pandas dataframe from dictionary
# 3. features_dataset() (fixture) -> ColumnSourceRegistrar() fall the register_file method and see the return type
# 4. MultiFeature(features_dataframe, features_dataset) (fixture) -> MultiFeature object with dataset and dataframe

@pytest.fixture()
def features_dataframe(mock_proto):
    return pd.DataFrame(mock_proto)

@pytest.fixture()
def features_dataset():
    registrar = MagicMock(return_value=Registrar())
    # or it could be SourceVariantResource
    source = MagicMock(return_value=SourceVariant)
    return ColumnSourceRegistrar(registrar, source)

@pytest.mark.parametrize(
    "name, variant",
    [
        ("n", "v"),
    ],
)
def test_dataframe(name, variant, client, mock_proto, features_dataframe, features_dataset):
    client.dataframe = MagicMock(return_value=features_dataframe)
    client.dataset = MagicMock(return_value=features_dataset)
    df = client.dataframe(name, variant)

    assert df.equals(mock_stats_df)

@pytest.fixture()
def multi_feature(features_dataframe, features_dataset):
    return ff.MultiFeature(features_dataframe, features_dataset)

# test_multi_feature.py

def test_multi_feature(multi_feature, features_dataframe, features_dataset):
    assert multi_feature.dataframe == features_dataframe
    assert multi_feature.dataset == features_dataset

import os
from datetime import timedelta

import pytest
import pandas as pd
import featureform as ff
from featureform import local

real_path = os.path.realpath(__file__)
dir_path = os.path.dirname(real_path)
SOURCE_FILE = f"{dir_path}/test_files/input_files/lag_features_testing.csv"

def setup():
    ff.register_user("featureformer").make_default_owner()

    local = ff.register_local()

    test_source_file = local.register_file(
        name="source_file",
        variant="testing",
        description="source file",
        path=SOURCE_FILE
    )

    @local.df_transformation(name=f"source_entity", 
                            variant="testing",
                            inputs=[(f"source_file", "testing")])
    def source_entity_transformation(df):
        """ the source dataset with entity """
        df["entity"] = "farm"
        return df

    @local.df_transformation(name=f"source_transformation", 
                            variant="testing",
                            inputs=[(f"source_entity", "testing")])
    def source_transformation(df):
        return df

    entity = ff.register_entity("entity")

    # Register a column from our transformation as a feature
    source_transformation.register_resources(
        entity=entity,
        entity_column="entity",
        timestamp_column="timestamp",
        inference_store=local,
        features=[
            {"name": "testing_feature", "variant": "testing", "column": "value", "type": "float32"},
        ],
    )

    # Register label from our base Transactions table
    source_entity_transformation.register_resources(
        entity=entity,
        entity_column="entity",
        timestamp_column="timestamp",
        labels=[
            {"name": f"testing_label", "variant": "testing", "column": "score", "type": "float32"},
        ],
    )

    ff.register_training_set(
        "testing_training", "no_lag_features",
        label=(f"testing_label", "testing"),
        features=[
            (f"testing_feature", "testing"),
        ],
    )

    ff.register_training_set(
        "testing_training", "one_lag_features",
        label=(f"testing_label", "testing"),
        features=[
            (f"testing_feature", "testing"),
            {"feature": f"testing_feature", "variant": "testing", "name": "testing_feature_lag_1h", "lag": timedelta(hours=1)},
        ],
    )

    ff.register_training_set(
        "testing_training", "three_lag_features",
        label=(f"testing_label", "testing"),
        features=[
            (f"testing_feature", "testing"),
            {"feature": f"testing_feature", "variant": "testing", "name": "testing_feature_lag_1h", "lag": timedelta(hours=1)},
            {"feature": f"testing_feature", "variant": "testing", "name": "testing_feature_lag_2h", "lag": timedelta(hours=2)},
            {"feature": f"testing_feature", "variant": "testing", "lag": timedelta(seconds=10800)},
        ],
    )

    resource_client = ff.ResourceClient(local=True)
    resource_client.apply()



@pytest.fixture()
def source_df():
    df = pd.read_csv(SOURCE_FILE)
    df["timestamp"] = pd.to_datetime(df.timestamp)
    df.rename(columns = {'value':'testing_feature.testing', 'timestamp':'label_timestamp', 'score': 'label'}, inplace=True)
    return df

@pytest.fixture()
def df_no_lag(source_df):
    return source_df[['testing_feature.testing', 'label_timestamp', 'label']]

@pytest.fixture()
def df_one_lag(source_df):
    source_df.set_index("label_timestamp", inplace=True)
    source_df["testing_feature_lag_1h"] = source_df["testing_feature.testing"].shift(freq=timedelta(hours=1))
    source_df.reset_index(inplace=True)
    return source_df[['testing_feature.testing', 'testing_feature_lag_1h', 'label_timestamp', 'label']]

@pytest.fixture()
def df_three_lags(source_df):
    source_df.set_index("label_timestamp", inplace=True)
    source_df["testing_feature_lag_1h"] = source_df["testing_feature.testing"].shift(freq=timedelta(hours=1))
    source_df["testing_feature_lag_2h"] = source_df["testing_feature.testing"].shift(freq=timedelta(hours=2))
    source_df["testing_feature_testing_lag_3_00_00"] = source_df["testing_feature.testing"].shift(freq=timedelta(hours=3))
    source_df.reset_index(inplace=True)
    return source_df[['testing_feature.testing', 'testing_feature_lag_1h', 'testing_feature_lag_2h', 'testing_feature_testing_lag_3_00_00', 'label_timestamp', 'label']]

@pytest.mark.parametrize(
    "training_set_variant, expected_df_name",
    [
        ("no_lag_features", "df_no_lag"),
        ("one_lag_features", "df_one_lag"),
        ("three_lag_features", "df_three_lags"),
    ]
)
def test_include_label_timestamp(training_set_variant, expected_df_name, request):
    expected_df = request.getfixturevalue(expected_df_name)
    serving_client = ff.ServingClient(local=True)
    dataset = serving_client.training_set('testing_training', training_set_variant, include_label_timestamp=True)
    df = dataset.pandas()
    pd.set_option("display.max_columns", None)
    print(df.head())
    print(expected_df.head())
    print("---"*10)

    assert df.equals(expected_df), f"The dataframes do not match. Expected: {expected_df.head()}, Got: {df.head()}"

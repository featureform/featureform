#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

import pytest

from featureform import FeatureColumnResource


# conftest.py
# 1. data_dictionary() (fixture) -> {columns: [""], data: [[],[]]}
# 2. features_dataframe(data_dictionary) (fixture) -> pandas dataframe from dictionary
# 3. primary_dataset() (fixture) -> ColumnSourceRegistrar() fall the register_file method and see the return type
# 4. MultiFeature(features_dataframe, features_dataset) (fixture) -> MultiFeature object with dataset and dataframe


def test_multi_feature(multi_feature):
    assert multi_feature.tags == []


@pytest.mark.parametrize(
    "input_str, expected_str",
    [
        ("column1", "column1"),
        ('"column2"', "column2"),
        ('"column3', "column3"),
        ('""', ""),
    ],
)
def test_clean_name(multi_feature, input_str, expected_str):
    result = multi_feature._clean_name(input_str)
    assert result == expected_str


@pytest.mark.parametrize(
    "input_str, has_quotes, expected_str",
    [
        ("column1", True, '"column1"'),
        ('"column2"', True, '"column2"'),
        ('""', True, '""'),
        ('"column3', False, "column3"),
        ('""', False, ""),
    ],
)
def test_modify_column_name(multi_feature, input_str, has_quotes, expected_str):
    result = multi_feature._modify_column_name(input_str, has_quotes)
    assert result == expected_str


@pytest.mark.parametrize(
    "include_cols, exclude_cols, expected_list",
    [
        ([], [], ["a", "b", "c", "ts"]),
        (["a"], ["ts"], ["a"]),
        (["a", "b", "ts"], ["c"], ["a", "b", "ts"]),
        ([], ["b", "c"], ["a", "ts"]),
    ],
)
def test_get_feature_columns_no_ts(
    include_cols, exclude_cols, expected_list, multi_feature, features_dataframe
):
    result = multi_feature._get_feature_columns(
        df=features_dataframe,
        include_columns=include_cols,
        exclude_columns=exclude_cols,
        entity_column="entity",
        timestamp_column="",
    )
    assert set(result) == set(expected_list)


@pytest.mark.parametrize(
    "include_cols, exclude_cols, expected_list",
    [
        ([], [], ["a", "b", "c"]),
        (["a"], [], ["a"]),
        (["a", "b"], ["c"], ["a", "b"]),
        ([], ["b", "c"], ["a"]),
    ],
)
def test_get_feature_columns(
    include_cols, exclude_cols, expected_list, multi_feature, features_dataframe
):
    result = multi_feature._get_feature_columns(
        df=features_dataframe,
        include_columns=include_cols,
        exclude_columns=exclude_cols,
        entity_column="entity",
        timestamp_column="ts",
    )
    assert set(result) == set(expected_list)


@pytest.mark.parametrize(
    "include_cols, exclude_cols",
    [
        (["a", "b", "c"], ["c"]),
        (["a", "d"], []),
        ([], ["c", "d"]),
    ],
)
def test_get_feature_columns_errors(
    include_cols, exclude_cols, multi_feature, features_dataframe
):
    with pytest.raises(ValueError):
        multi_feature._get_feature_columns(
            df=features_dataframe,
            include_columns=include_cols,
            exclude_columns=exclude_cols,
            entity_column="entity",
            timestamp_column="ts",
        )


def test_multi_feature_is_iterable(multi_feature):
    for feature in multi_feature:
        assert isinstance(feature, FeatureColumnResource)

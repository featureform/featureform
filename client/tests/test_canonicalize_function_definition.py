#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

import asyncio

import pytest

from featureform import canonicalize_function_definition

"""
DO NOT FORMAT THIS FILE
"""

# fmt: off


def my_function1(
        df
):
    # This is a comment
    df = df.head(5)  # Take top 5 rows
    if df.empty:  # Check if empty

        return df  # Return empty dataframe
    return df  # Final return


expected_output1 = """def my_function1(df):
    df = df.head(5)
    if df.empty:
        return df
    return df"""


def my_function2(df):
    # This function processes a dataframe
    """
    some crazy doc string
    :param df:
    :return:
    """
    df = df.dropna()
    # Remove missing values
    if df.empty:  # Check if DataFrame is empty
        return df  # Return the empty dataframe
    return df  # Returning the dataframe


expected_output2 = """def my_function2(df):
    df = df.dropna()
    if df.empty:
        return df
    return df"""


def my_function3(df):
    df = df[df['col'] > 0]  # Keep only positive values
    if (df.empty):  # Check if df is empty
        # If empty, return df
        return (df)
    df['new_col'] = df['col'] * 2  # Create a new column
    return df  # Return the modified dataframe


expected_output3 = """def my_function3(df):
    df = df[df['col'] > 0]
    if df.empty:
        return df
    df['new_col'] = df['col'] * 2
    return df"""


def my_function_with_decorator(df):
    @some_decorator
    def inner_function(x):
        return x * 2
    return inner_function(df)


expected_output_with_decorator = """def my_function_with_decorator(df):

    @some_decorator
    def inner_function(x):
        return x * 2
    return inner_function(df)"""


def my_function_with_nested(df):
    def helper(y):
        return y + 1
    return df.apply(helper)

expected_output_with_nested = """def my_function_with_nested(df):

    def helper(y):
        return y + 1
    return df.apply(helper)"""


def my_function_with_defaults(a, b=10):
    return a + b


expected_output_with_defaults = """def my_function_with_defaults(a, b=10):
    return a + b"""


def my_function_with_annotations(a: int, b: str) -> bool:
    return str(a) == b


expected_output_with_annotations = """def my_function_with_annotations(a: int, b: str) ->bool:
    return str(a) == b"""


def my_function_with_lambda(df):
    return df.apply(lambda x: x * 2)


expected_output_with_lambda = """def my_function_with_lambda(df):
    return df.apply(lambda x: x * 2)"""


def my_function_with_control_flow(df):
    try:
        df['new_col'] = df['col'] / df['divider']
    except ZeroDivisionError:
        df['new_col'] = None
    finally:
        return df


expected_output_with_control_flow = """def my_function_with_control_flow(df):
    try:
        df['new_col'] = df['col'] / df['divider']
    except ZeroDivisionError:
        df['new_col'] = None
    finally:
        return df"""


def my_function_with_multiline_string():
    sql_query = """
    SELECT *
    FROM table
    WHERE condition
    """
    return sql_query


expected_output_with_multiline_string = '''def my_function_with_multiline_string():
    sql_query = """
    SELECT *
    FROM table
    WHERE condition
    """
    return sql_query'''

global_var = 0


def my_function_with_globals():
    global global_var
    global_var += 1
    return global_var


expected_output_with_globals = """def my_function_with_globals():
    global global_var
    global_var += 1
    return global_var"""


@pytest.mark.parametrize(
    "my_function, expected_output",
    [
        (my_function1, expected_output1),
        (my_function2, expected_output2),
        (my_function3, expected_output3),
        (my_function_with_decorator, expected_output_with_decorator),
        (my_function_with_nested, expected_output_with_nested),
        (my_function_with_defaults, expected_output_with_defaults),
        (my_function_with_annotations, expected_output_with_annotations),
        (my_function_with_lambda, expected_output_with_lambda),
        (my_function_with_control_flow, expected_output_with_control_flow),
        (my_function_with_multiline_string, expected_output_with_multiline_string),
        (my_function_with_globals, expected_output_with_globals),
    ],
)
def test_canonicalize_function_definition(my_function, expected_output):
    normalized_code = canonicalize_function_definition(my_function)
    assert normalized_code == expected_output

# fmt: on

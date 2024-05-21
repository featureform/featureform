import featureform as ff
from behave import when


@when("I can register a training set based on the feature and label")
def step_impl(context):
    # TODO: Add unique name for training set
    training_set = ff.register_training_set(
        name="fraud_training_set",
        features=[context.user_feature],
        label=context.user_label,
    )
    context.client.apply(asynchronous=False, verbose=True)
    context.training_set = training_set


@then('I can serve the training set with "{expected_row_count}"')
def step_impl(context, expected_row_count):
    training_set = context.client.training_set(context.training_set)
    df = training_set.dataframe()

    assert (
        len(df) == int(expected_row_count)
    ), f"Expected {expected_row_count} rows but got {len(df)}"

    # assert column exists in dataframe
    feature_column_name = (
        f"feature__{context.user_feature.name}__{context.user_feature.variant}"
    )
    assert (
        feature_column_name in df.columns
    ), f"Expected '{feature_column_name}' column in dataframe; but dataframe has {df.columns} columns"
    assert (
        "label" in df.columns
    ), f"Expected 'label' column in dataframe; but dataframe has {df.columns} columns"

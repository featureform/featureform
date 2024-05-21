from behave import then


@then(
    'I can serve the feature with "{entity}" and "{expected_value}" of "{expected_type}"'
)
def step_impl(context, entity, expected_value, expected_type):
    feature = context.client.features(
        [(context.user_feature.name, context.user_feature.variant)],
        {"user": entity},
    )

    assert len(feature) == 1, f"Expected 1 feature but got {len(feature)}"

    if expected_type in ["int", "int32", "int64"]:
        assert feature[0] == int(
            expected_value
        ), f"Expected {expected_value} but got {feature[0]}"
    elif expected_type in ["float32", "float64"]:
        assert feature[0] == float(
            expected_value
        ), f"Expected {expected_value} but got {feature[0]}"
    elif expected_type == "string":
        assert (
            feature[0] == expected_value
        ), f"Expected {expected_value} but got {feature[0]}"
    elif expected_type == "bool":
        assert feature[0] == bool(
            expected_value
        ), f"Expected {expected_value} but got {feature[0]}"
    elif expected_type == "null":
        if expected_value == "empty":
            expected_value = None
        assert feature[0] == expected_value, f"Expected '' but got {feature[0]}"
    else:
        raise ValueError(f"Unexpected type: {expected_type}")

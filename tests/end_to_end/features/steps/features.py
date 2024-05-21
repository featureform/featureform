# import featureform as ff
# from behave import then


@then(
    'I can serve the feature with "{entity}" and "{expected_value}" of "{expected_type}"'
)
def step_impl(context, entity, expected_value, expected_type):
    feature = context.client.features(
        [(context.user_feature.name, context.user_feature.variant)],
        {"user": entity},
    )

    assert len(feature) == 1, f"Expected 1 feature but got {len(feature)}"

    if expected_type == "int":
        assert feature[0] == int(
            expected_value
        ), f"Expected {expected_value} but got {feature[0]}"
    else:
        raise ValueError(f"Unexpected type: {expected_type}")

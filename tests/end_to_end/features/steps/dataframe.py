from behave import when, then


@when('I register a "{transformation_type}" transformation with "{limit}" rows')
def step_impl(context, transformation_type, limit):
    if transformation_type == "df":
        if limit == "none":

            @context.offline_provider.df_transformation(inputs=[context.dataset])
            def transformation(df):
                return df

        elif limit == "0":

            @context.offline_provider.df_transformation(inputs=[context.dataset])
            def transformation(df):
                return df.limit(0)

        elif limit == "1":

            @context.offline_provider.df_transformation(inputs=[context.dataset])
            def transformation(df):
                return df.limit(1)

        else:
            raise ValueError(f"Unknown limit: {limit}")

        context.client.apply()
        context.transformation = transformation
    else:
        raise ValueError(f"Unknown transformation type: {transformation_type}")


@then('I can call client.dataframe on the transformation with "{expected_num_rows}"')
def step_impl(context, expected_num_rows):
    df = context.client.dataframe(context.transformation)
    assert len(df) == int(
        expected_num_rows
    ), f"Expected {expected_num_rows} rows, got {len(df)} rows."

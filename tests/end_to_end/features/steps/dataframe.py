from behave import when, then


@when('I register a "{transformation_type}" transformation with "{limit}" rows')
def step_impl(context, transformation_type, limit):
    if transformation_type == "df":
        if limit == "none":

            @context.offline_provider.df_transformation(inputs=[context.dataset])
            def transformation(df):
                return df

        else:

            @context.offline_provider.df_transformation()
            def transformation(df):
                return df.limit(int(limit))

        context.client.apply()
        context.transformation = transformation
    else:
        raise ValueError(f"Unknown transformation type: {transformation_type}")


@then('I can call client.dataframe on the transformation with "{limit}" rows')
def step_impl(context, limit):
    df = context.client.dataframe(context.transformation)
    assert df.count() == int(limit)

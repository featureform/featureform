from behave import when, then


@when("I get the source for the dataset")
def step_impl(context):
    dataset_name, dataset_variant = context.dataset.id()

    context.source = context.client.get_source(dataset_name, dataset_variant)


@then("I can register a transformation based on the source")
def step_impl(context):
    source = context.source

    @context.offline_provider.sql_transformation(inputs=[source])
    def get_source_transformation(src):
        return "SELECT * FROM {{ src }}"

    context.client.apply()
    context.transformation = get_source_transformation


@then("I can serve the transformation")
def step_impl(context):
    transformation = context.transformation

    df = context.client.dataframe(transformation)
    assert df is not None

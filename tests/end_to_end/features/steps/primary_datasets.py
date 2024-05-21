from behave import then


@then("I can serve the primary dataset with all column types as expected")
def step_impl(context):
    df = context.client.dataframe(context.dataset)

    assert len(df) == 1000, f"Expected 1000 rows but got {len(df)}"

    assert (
        df["int_col"].dtype == "int64"
    ), f"Expected int64 but got {df['int_col'].dtype}"
    assert (
        df["int32_col"].dtype == "int64"
    ), f"Expected int64 but got {df['int32_col'].dtype}"
    assert (
        df["int64_col"].dtype == "int64"
    ), f"Expected int64 but got {df['int64_col'].dtype}"
    assert (
        df["float32_col"].dtype == "float64"
    ), f"Expected float64 but got {df['float32_col'].dtype}"
    assert (
        df["float64_col"].dtype == "float64"
    ), f"Expected float64 but got {df['float64_col'].dtype}"
    assert (
        df["string_col"].dtype == "object"
    ), f"Expected object but got {df['string_col'].dtype}"
    assert (
        df["boolean_col"].dtype == "object"
    ), f"Expected object but got {df['boolean_col'].dtype}"
    assert (
        df["datetime_col"].dtype == "object"
    ), f"Expected object but got {df['datetime_col'].dtype}"
    assert (
        df["nil_col"].dtype == "object"
    ), f"Expected object but got {df['nil_col'].dtype}"

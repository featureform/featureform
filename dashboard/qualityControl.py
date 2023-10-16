import great_expectations as ge
from great_expectations.datasource import PandasDatasource

# Define the feature table (replace this with data source)
data_context = ge.data_context.DataContext("/path/to/your/data_context_directory")
data_asset_name = "your_feature_table"
data_source = PandasDatasource("pandas_datasource")
batch_kwargs = {"table": data_asset_name, "datasource": data_source}

# Create an expectation suite (You can define more detailed expectations)
data_context.create_expectation_suite(data_asset_name, overwrite=True)
# Add expectations to the suite
data_context.expect(data_asset_name, "expect_column_values_to_be_in_set", column="your_column", value_set=[1, 2, 3])

# Validate the batch and store the result
batch = data_context.get_batch(batch_kwargs, data_context.get_expectation_suite(data_asset_name))
results = data_context.run_validation_operator(
    "action_list_operator", [batch], run_id="batch_" + data_asset_name
)

# Check results and handle them as needed
if results["success"]:
    print("Data quality checks passed.")
else:
    print("Data quality checks failed.")
    for result in results["results"]:
        print(result)

# Now we can now display these results in your dashboard's data quality page

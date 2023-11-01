import great_expectations as ge
from great_expectations.datasource import PandasDatasource

def run_data_quality_checks(data_context_dir, feature_table, column, value_set):
    try:
        # Create a Great Expectations data context
        data_context = ge.data_context.DataContext(data_context_dir)
        data_source = PandasDatasource("pandas_datasource")
        
        # Define batch parameters
        batch_kwargs = {"table": feature_table, "datasource": data_source}

        # Create an expectation suite for the feature table
        data_context.create_expectation_suite(feature_table, overwrite=True)

        # Add expectations to the suite (customizable)
        data_context.expect(
            feature_table,
            "expect_column_values_to_be_in_set",
            column=column,
            value_set=value_set
        )

        # Validate the batch and store the result
        batch = data_context.get_batch(batch_kwargs, data_context.get_expectation_suite(feature_table))
        results = data_context.run_validation_operator("action_list_operator", [batch], run_id="batch_" + feature_table)

        # Check results and handle them as needed
        if results["success"]:
            print(f"Data quality checks passed for {feature_table}.")
        else:
            print(f"Data quality checks failed for {feature_table}.")
            for result in results["results"]:
                print(result)
    except Exception as e:
        print(f"An error occurred: {str(e)}")

# Usage example for a specific feature
run_data_quality_checks("/path/to/data_context", "your_feature_table", "your_column", [1, 2, 3])

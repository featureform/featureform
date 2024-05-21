Feature: Data within Featureform
  Scenario Outline: Registering a feature and training set with different data types
    Given Featureform is installed
    When I turn on autovariants
    And I create a "hosted" "insecure" client for "localhost:7878"
    And I register an offline provider of type "<offline_provider_type>"
    And I register an online provider of type "<online_provider_type>"
    And I register a dataset located at "<dataset_path>"
    And I register a feature on "<feature_column>" with type "<feature_type>" with "<entity_column>", "<timestamp_column>", and "<label_column>"
    And I can register a training set based on the feature and label
    Then I can serve the primary dataset with all column types as expected
    And I can serve the feature with "<entity>" and "<expected_value>" of "<expected_type>"
    And I can serve the training set with "<expected_row_count>"
    And I turn off autovariants

    Examples: Spark Data Types with Redis
      | online_provider_type | offline_provider_type | dataset_path                                                   | feature_column | feature_type | entity_column | timestamp_column | label_column | entity        | expected_value                     | expected_type | expected_row_count |
      | redis                | spark                 | s3://featureform-spark-testing/datasets/data_types_dataset.csv | int_col        | int          | username      | datetime_col     | boolean_col  | wiseposition  | -21524                             | int           | 1000               |
      | redis                | spark                 | s3://featureform-spark-testing/datasets/data_types_dataset.csv | int32_col      | int32        | username      | datetime_col     | boolean_col  | maturesound   | 1671621217                         | int32         | 1000               |
      | redis                | spark                 | s3://featureform-spark-testing/datasets/data_types_dataset.csv | int64_col      | int64        | username      | datetime_col     | boolean_col  | richexistence | 1136335578646221838                | int64         | 1000               |
      | redis                | spark                 | s3://featureform-spark-testing/datasets/data_types_dataset.csv | float32_col    | float32      | username      | datetime_col     | boolean_col  | woozyjar      | 2.4922891928141537e-39             | float32       | 1000               |
      | redis                | spark                 | s3://featureform-spark-testing/datasets/data_types_dataset.csv | float64_col    | float64      | username      | datetime_col     | boolean_col  | dynamictrip   | 1.80046184076813e+198              | float64       | 1000               |
      | redis                | spark                 | s3://featureform-spark-testing/datasets/data_types_dataset.csv | string_col     | string       | username      | datetime_col     | boolean_col  | triteframe    | In sed lacus ac mi placerat congue | string        | 1000               |
      | redis                | spark                 | s3://featureform-spark-testing/datasets/data_types_dataset.csv | nil_col        | null         | username      | datetime_col     | boolean_col  | briefshape    | nil                                | null          | 1000               |

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
    And I can serve the training set
    And I turn off autovariants

    Examples: Spark Data Types with Redis
      | online_provider_type | offline_provider_type | dataset_path                                                   | feature_column | feature_type | entity_column | timestamp_column | label_column | entity        | expected_value                                                                | expected_type |
      | redis                | spark                 | s3://featureform-spark-testing/datasets/data_types_dataset.csv | int_col        | int          | username      | datetime_col     | boolean_col  | wiseposition  | -4767                                                                         | int           |
      | redis                | spark                 | s3://featureform-spark-testing/datasets/data_types_dataset.csv | int32_col      | int32        | username      | datetime_col     | boolean_col  | maturesound   | -1524968518                                                                   | int32         |
      | redis                | spark                 | s3://featureform-spark-testing/datasets/data_types_dataset.csv | int64_col      | int64        | username      | datetime_col     | boolean_col  | richexistence | 7240463747944887165                                                           | int64         |
      | redis                | spark                 | s3://featureform-spark-testing/datasets/data_types_dataset.csv | float32_col    | float32      | username      | datetime_col     | boolean_col  | woozyjar      | 7.56708730917959E+24                                                          | float32       |
      | redis                | spark                 | s3://featureform-spark-testing/datasets/data_types_dataset.csv | float64_col    | float64      | username      | datetime_col     | boolean_col  | dynamictrip   | -3.5062166438966E+39                                                          | float64       |
      | redis                | spark                 | s3://featureform-spark-testing/datasets/data_types_dataset.csv | string_col     | string       | username      | datetime_col     | boolean_col  | triteframe    | Vestibulum lectus odio, egestas eu tellus efficitur, accumsan ullamcorper eni | string        |
      | redis                | spark                 | s3://featureform-spark-testing/datasets/data_types_dataset.csv | nil_col        | null         | username      | datetime_col     | boolean_col  | briefshape    | empty                                                                         | null          |

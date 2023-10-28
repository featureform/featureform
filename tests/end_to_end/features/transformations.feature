Feature: Transformations

  Scenario Outline: Chained Spark Transformations
    Given Featureform is installed
    When I create a "hosted" "insecure" client for "localhost:7878"
    And I generate a random variant name
    And I upload a "small" "csv" file to "<storage_provider>"
    And I register "<storage_provider>" filestore with bucket "<bucket>" and root path "<root_path>"
    And I register databricks
    And I register the file
    When I register a "<transformation_type>" transformation named "first_transformation" from "transactions"
    When I register a "<transformation_type>" transformation named "second_transformation" from "first_transformation"
    Then I should be able to pull the transformation as a dataframe

    Examples:
      | transformation_type | storage_provider   |         bucket            |    root_path               |
      | DF                 | azure               |         test              |      behave                |
      | DF                 | azure               |         test              |       empty                |
      | DF                 | azure               |         test              |     featureform            |
      | DF                 | azure               |         test              | featureform/featureform    |
      | DF                 | s3                  | featureform-spark-testing |      behave                |
      | DF                 | s3                  | featureform-spark-testing |       empty                |
      | DF                 | s3                  | featureform-spark-testing |     featureform            |
      | DF                 | s3                  | featureform-spark-testing | featureform/featureform    |
#      | DF                 | gcs                 |    featureform-test       |          behave            | TODO: Enable Later

  Scenario Outline: Single Spark Transformations
    Given Featureform is installed
    When I create a "hosted" "insecure" client for "localhost:7878"
    And I generate a random variant name
    And I upload a "small" "csv" file to "<storage_provider>"
    And I register "<storage_provider>" filestore with bucket "<bucket>" and root path "behave"
    And I register databricks
    And I register the file
    When I register a "<transformation_type>" transformation named "first_transformation" from "transactions"
    Then I should be able to pull the transformation as a dataframe

    Examples:
    | transformation_type | storage_provider | bucket |
    | SQL                 | azure               | test |
    | SQL                 | s3               | featureform-spark-testing |
#    | SQL                 | gcs               | test |

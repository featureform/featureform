Feature: Transformations

  Scenario Outline: Spark Transformations
    Given Featureform is installed
    When I create a "hosted" "insecure" client for "localhost:7878"
    And I generate a random variant name
    And I upload a "small" "csv" file to "<storage_provider>"
    And I register "<storage_provider>" filestore with bucket "<bucket>" and root path "behave"
    And I register databricks
    And I register the file
    When I register a "<transformation_type>" transformation
    Then I should be able to pull the transformation as a dataframe

    Examples:
    | transformation_type | storage_provider | bucket |
    | SQL                 | azure               | test |
    | SQL                 | s3               | featureform-spark-testing |
    | SQL                 | gcs               | test |

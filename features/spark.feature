Feature: Showing the ability to register and utilize a Spark provider

Scenario: Register a Spark offline provider
    Given Featureform is installed
    And Featureform is hosted on k8s
    When we register a Spark provider
    And a Spark parquet file is registered
    And Spark SQL transformation is registered
    And a label is registered
    And training set is registered
    Then we can serve the training set
Feature: Model Registration
  Scenario: Registerign a model with non-existing training set
    Given Featureform is installed
    When I create a "hosted" "insecure" client for "localhost:7878"
    And I register a "<offline_provider>"
    And I register a dataset located at "<dataset_path>"
    And I register a feature based on the column "<column_name>" with type "<feature_type>"
    Then I can register a training set based on the feature
    And I can serve the non-existing training set with the model
    And I can serve the registered training set with the model

    Examples:
      | offline_provider | dataset_path | column_name | feature_type |
      |     Postgres     | transactions |    amount   |    float64   |
      |     Postgres     | Transactions |    amount   |    float64   |
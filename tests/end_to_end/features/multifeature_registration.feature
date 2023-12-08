Feature: MultiFeature Registration

  Scenario: Registering all but one feature from one table
    Given Featureform is installed
    And The Postgres Quickstart container is running
    And Redis is running
    When I register postgres
    And I register a table from postgres
    And I create a dataframe from a serving client
    Then I define a User and register multiple features excluding one
    Then I should be able to serve a batch of features

  Scenario: Registering three features from one large table
    Given Featureform is installed
    And The Postgres Quickstart container is running
    And Redis is running
    When I register postgres
    And I register a table from postgres
    And I create a dataframe from a serving client
    Then I define a User and register multiple but not all features, with no timestamp column
    Then I should be able to serve a batch of features
Feature: Batch Serving

  @wip
  Scenario: Serving Batch Features (Snowflake)
    Given Featureform is installed
    And The Snowflake container is running
    And Redis is running
    When I register Snowflake
    And I register redis
    And I register the tables from the database
    And I define a User and register features
    And I create a "hosted" "insecure" client for "localhost:7878"
    Then I serve batch features
    And I can get a list containing the entity name and a tuple with all the features
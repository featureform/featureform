Feature: Batch Serving

  @wip
  Scenario: Serving Batch Features (Snowflake)
    Given Featureform is installed
    And The Snowflake container is running
    And Redis is running
    When I register a Snowflake and with a "0884D0DD-468D-4C3A-8109-3C2BAAD72EF7" and "PUBLIC"
    And I register redis
    And I register the tables from the database
    And I define a User and register features
    And I create a "hosted" "insecure" client for "localhost:7878"
    Then I serve batch features
    And I can get a list containing the entity name and a tuple with all the features
Feature: On Demand Features
  Scenario: Basic On Demand Feature
    Given Featureform is installed
    When I create a "hosted" "insecure" client for "localhost:7878"
    And I generate a random variant name
    And I register an ondemand feature
    Then I can pull the ondemand feature
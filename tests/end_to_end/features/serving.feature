Feature: Batch Serving

  @wip
  Scenario: Serving Batch Features (Snowflake)
    Given Featureform is installed
    And The Snowflake container is running
    And Redis is running
    When I register Snowflake
    And I register redis
    And I register the tables from the database
    And I define a Snowflake User and register features
    And I create a "hosted" "insecure" client for "localhost:7878"
    Then I serve batch features <("table1_feature", "variant_628138"), ("table2_feature", "variant_628138"), ("table3_feature", "variant_628138"), ("table4_feature", "variant_628138")>
    And I can get a list containing the entity name and a tuple with all the features


  @wip
  Scenario: Serving Batch Features (Spark)
    Given Featureform is installed
    And The Databricks env variables are available
    And The S3 env variables are available
    And Redis is running
    When I register Spark with Databricks S3
    And I register redis
    And I register the files from the database
    And I define a Spark User and register features
    And I create a "hosted" "insecure" client for "localhost:7878"
    Then I serve batch features <("transaction_feature", "variant_43636"), ("balance_feature", "variant_436536"), ("perc_feature", "variant_436536")>
    And I can get a list containing the entity name and a tuple with all the features
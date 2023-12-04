Feature: Batch Serving

  Scenario: Serving Batch Features (Snowflake)
    Given Featureform is installed
    And The Snowflake env variables are available
    When I create a "hosted" "insecure" client for "localhost:7878"
    And I register Snowflake
#    And I register redis
    And I register the tables from the database
    And I define a SnowflakeUser and register features
    Then I serve batch features for snowflake
    And I can get a list containing the entity name and a tuple with all the features

  Scenario: Serving Batch Features (Spark)
    Given Featureform is installed
    When I create a "hosted" "insecure" client for "localhost:7878"
    And I register "s3" filestore with bucket "featureform-spark-testing" and root path "behave"
    And I register databricks
#    And I register redis
    And I register the files from the database
    And I define a SparkUser and register features
    Then I serve batch features for spark
    And I can get a list containing the entity name and a tuple with all the features
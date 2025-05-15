#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

Feature: Batch Serving

  # @skip
  # Scenario: Serving Batch Features (Snowflake)
  #   Given Featureform is installed
  #   And The Snowflake env variables are available
  #   When I create a "hosted" "insecure" client for "localhost:7878"
  #   And I generate a random variant name
  #   And I register Snowflake
  #   And I register the tables from the database
  #   And I define a SnowflakeUser and register features
  #   Then I serve batch features for snowflake
  #   And I can get a list containing the entity name and a tuple with all the features from "snowflake"

#  Scenario: Serving Batch Features (Spark)
#    Given Featureform is installed
#    When I create a "hosted" "insecure" client for "localhost:7878"
#    And I generate a random variant name
#    And I register "s3" filestore with bucket "ff-spark-testing" and root path "behave"
#    And I get or register databricks
#    And I register the "short" files from the database
#    And I define a SparkUser and register features
#    Then I serve batch features for spark
#    And I can get a list containing the entity name and a tuple with all the features from "spark"
#
#  Scenario: Serving Batch Features (Spark with Submit Params Exceeding 10K Bytes)
#    Given Featureform is installed
#    When I create a "hosted" "insecure" client for "localhost:7878"
#    And I generate a random variant name
#    And I register "s3" filestore with bucket "ff-spark-testing" and root path "behave"
#    And I get or register databricks
#    And I register the "long" files from the database
#    And I define a SparkUser and register features
#    Then I serve batch features for spark with submit params that exceed the 10K-byte API limit
#    And I can get a list containing the correct number of features
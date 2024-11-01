#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

Feature: Snowflake Primary Tables with Spark
  @av
  Scenario: Reading Snowflake tables with Spark
    Given Featureform is installed
    And The Snowflake env variables are available
    When I create a "hosted" "insecure" client for "localhost:7878"
    And I register Snowflake with "DEMO" database
    And I register the "TRANSACTIONS" table with Snowflake
    And I serve the snowflake table with client dataframe with expected "1048124" records
    And I get or register EMR with S3
    Then I can create spark "sql" transformation with Snowflake table as source
    And I can serve the transformation with client dataframe
    Then I can create spark "df" transformation with Snowflake table as source
    And I can serve the transformation with client dataframe

  @av
  Scenario: Snowflake Transformation with FF_LAST_RUN_TIMESTAMP
    Given Featureform is installed
    And The Snowflake env variables are available
    When I create a "hosted" "insecure" client for "localhost:7878"
    And I register Snowflake with "DEMO" database
    And I register the "TRANSACTIONS" table with Snowflake
    Then I can create sql transformation with FF_LAST_RUN_TIMESTAMP
    And I can serve the transformation with client dataframe

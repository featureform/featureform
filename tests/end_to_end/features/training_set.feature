#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

Feature: Training Sets

  @wip
  Scenario: Training Set Label Column (Postgres)
    Given Featureform is installed
    And The Postgres Quickstart container is running
    And Redis is running
    When I create a "hosted" "insecure" client for "localhost:7878"
    And I pull the Docker Quickstart as "definitions.py"
    And I apply "definitions.py" with a "hosted" "insecure" CLI for "localhost:7878"
    Then The training set will have a label column named label

  Scenario: Training Set Label Column Databricks
    Given Featureform is installed
    When I create a "hosted" "insecure" client for "localhost:7878"
    And I generate a random variant name
    And I upload a "small" "csv" file to "s3"
    And I get or register redis
    And I register "s3" filestore with bucket "ff-spark-testing" and root path "behave"
    And I get or register databricks
    And I register the file
    Then I should be able to pull the file as a dataframe
    When I register a "DF" transformation named "first_transformation" from "transactions"
    Then I should be able to pull the transformation as a dataframe
    When I register a feature from a "transformation"
    When I register a label from a "transformation"
    And I register a training set
    Then I should be able to pull the trainingset as a dataframe
    Then The training set will have a label column named label
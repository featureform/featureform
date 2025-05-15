#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

Feature: Client API
    Scenario: Data Source Columns (SQL Provider)
        Given Featureform is installed
        When I create a "hosted" "insecure" client for "localhost:7878"
        When I register postgres
        And I generate a random variant name
        And I register a table from postgres
        Then I should get the columns for the data source from "postgres"

    Scenario: Data Source Columns (Spark)
        Given Featureform is installed
        When I create a "hosted" "insecure" client for "localhost:7878"
        And I generate a random variant name
        And I register "s3" filestore with bucket "ff-spark-testing" and root path "data"
        And I get or register databricks
        And I register transactions_short.csv
        Then I should get the columns for the data source from "spark"

    Scenario: Reusing Providers
        Given Featureform is installed
        When I create a "hosted" "insecure" client for "localhost:7878"
        And I generate a random variant name
        And I register "s3" filestore with bucket "ff-spark-testing" and root path "data"
        And I get or register databricks
        Then I should be able to get spark provider
        And I should be able to register transactions_short.csv
        And I should be able to get the data of the resource

    Scenario: Reusing Sources
        Given Featureform is installed
        When I create a "hosted" "insecure" client for "localhost:7878"
        And I generate a random variant name
        And I register "s3" filestore with bucket "ff-spark-testing" and root path "data"
        And I get or register databricks
        And I register transactions_short.csv
        Then I should be able to get the resource
        And I should be able to get the data of the resource

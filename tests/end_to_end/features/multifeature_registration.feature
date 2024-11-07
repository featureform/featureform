#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

Feature: MultiFeature Registration

  @MultiFeature
  Scenario: Registering all but one feature from one table
    Given Featureform is installed
    When I create a "hosted" "insecure" client for "localhost:7878"
    And I register postgres
    And I generate a random variant name
    And I register a table from postgres
    And I create a dataframe from a serving client
    And I get or register redis
    Then I define a User and register multiple features excluding one
    Then I should be able to serve a batch of features

  @MultiFeature
  Scenario: Registering three features from one large table
    Given Featureform is installed
    When I create a "hosted" "insecure" client for "localhost:7878"
    And I register postgres
    And I generate a random variant name
    And I register a table from postgres
    And I create a dataframe from a serving client
    And I get or register redis
    Then I define a User and register multiple but not all features, with no timestamp column
    Then I should be able to serve a batch of features
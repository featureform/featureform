#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

Feature: On Demand Features
  Scenario: Basic On Demand Feature
    Given Featureform is installed
    When I create a "hosted" "insecure" client for "localhost:7878"
    And I generate a random variant name
    And I register an ondemand feature
    Then I can pull the ondemand feature
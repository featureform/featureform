#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

@av
Feature: AutoVariants

  Scenario Outline: Same auto variant for same transformation
    Given Featureform is installed
    When I create a "hosted" "insecure" client for "localhost:7878"
    And I register "s3" filestore with bucket "ff-spark-testing" and root path "data"
    And I get or register databricks
    And I register transactions_short.csv
    And I register "trans_av_1" transformation with auto variant of type "<transformation_type>"
    Then I should be able to reuse the same variant for the same "trans_av_1" transformation of type "<transformation_type>"
    And I can get the transformation as df

    Examples:
      | transformation_type |
      | sql                 |
      | df                  |

  Scenario: User-defined variant and auto variant for same transformation
    Given Featureform is installed
    When I create a "hosted" "insecure" client for "localhost:7878"
    And I register "s3" filestore with bucket "ff-spark-testing" and root path "data"
    And I get or register databricks
    And I register transactions_short.csv
    And I register a transformation with user-provided variant
    Then I should be able to register a new auto variant transformation
    And I can get the transformation as df

  Scenario Outline: Different auto variant for different transformation
    Given Featureform is installed
    When I create a "hosted" "insecure" client for "localhost:7878"
    And I register "s3" filestore with bucket "ff-spark-testing" and root path "data"
    And I get or register databricks
    When I register transactions_short.csv
    And I register "trans_av_2" transformation with auto variant of type "<transformation_type>"
    Then I should be able to register a modified "trans_av_2" transformation with new auto variant of type "<transformation_type>"
    And I can get the transformation as df

    Examples:
      | transformation_type |
      | sql                 |
      | df                  |

  Scenario: Create auto variant dataset and register a new dataset with user-defined variant
    Given Featureform is installed
    When I create a "hosted" "insecure" client for "localhost:7878"
    And I register "s3" filestore with bucket "ff-spark-testing" and root path "data"
    And I get or register databricks
    And I register transactions_short.csv
    Then I should be able to register a source with user-defined variant
    And I can get the source as df

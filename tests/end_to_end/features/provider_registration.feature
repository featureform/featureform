#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

Feature: Provider Registration

  @wip
  Scenario Outline: S3 Registration
    Given Featureform is installed
    When I create a "hosted" "insecure" client for "localhost:7878"
    And I create dummy "<cloud_provider>" credentials
    And I register S3 with name "<name>", bucket_name "<bucket_name>", bucket_region "<bucket_region>", path "<path>"
    Then An exception that "matches" "<exception>" should be raised
    Examples:
      | name | cloud_provider | bucket_name | bucket_region | path | exception                                                                                                                                                                                    |
      | test | AWS            | test        | us-east-1     | test | None                                                                                                                                                                                         |
      | test | GCP            | test        | us-east-1     | test | type of argument "credentials" must be one of (featureform.resources.AWSStaticCredentials, featureform.resources.AWSAssumeRoleCredentials); got featureform.resources.GCPCredentials instead |

  @wip
  Scenario Outline: GCS Registration
    Given Featureform is installed
    When I create a "hosted" "insecure" client for "localhost:7878"
    And I create dummy "<cloud_provider>" credentials
    And I register GCS with name "<name>", bucket_name "<bucket_name>", path "<path>"
    Then An exception that "matches" "<exception>" should be raised

    Examples:
      | name | cloud_provider | bucket_name | path | exception                                                                                                                                             |
      | test | AWS            | test        | test | type of argument "credentials" must be featureform.providers.credentials.GCPCredentials; got featureform.providers.credentials.AWSCredentials instead |
      | test | GCP            | test        | test | None                                                                                                                                                  |

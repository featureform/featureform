#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

Feature: Credentials

  @wip
  Scenario Outline: Create AWS Credentials
    Given Featureform is installed
    When I create the AWS credentials with an access key "<access_key>" and secret key "<secret_key>"
    Then An exception that "matches" "<exception>" should be raised
    Examples:
      | access_key | secret_key | exception                                         |
      | AXASCASCA  | asdvadvadv | None                                              |
      | empty      | asdvadvadv | 'AWSStaticCredentials' access_key cannot be empty |
      | AXASCASCA  | empty      | 'AWSStaticCredentials' secret_key cannot be empty |

  @wip
  Scenario Outline: Create GCP Credentials
    Given Featureform is installed
    When I create the GCP credentials with a project id "<project_id>" and credentials path "<credentials_path>"
    Then An exception that "matches" "<exception>" should be raised
    Examples:
      | project_id | credentials_path            | exception                                                                       |
      | my-project | test_files/dummy_creds.json | None                                                                            |
      | empty      | test_files/dummy_creds.json | 'GCPCredentials' project_id cannot be empty                                     |
      | my-project | empty                       | 'GCPCredentials' credentials_path cannot be empty or credentials cannot be None |
      | my-project | invalid/file.json           | 'GCPCredentials' credentials_path 'invalid/file.json' file not found            |
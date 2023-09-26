Feature: Credentials

  Scenario Outline: Create AWS Credentials
    Given Featureform is installed
    When I create the AWS credentials with an access key "<access_key>" and secret key "<secret_key>"
    Then An exception that "matches" "<exception>" should be raised
    Examples:
      | access_key | secret_key | exception |
      | AXASCASCA  | asdvadvadv | None  |
      | empty  | asdvadvadv | 'AWSCredentials' access_key cannot be empty  |
      |  AXASCASCA | empty | 'AWSCredentials' secret_key cannot be empty  |


  Scenario Outline: Create GCP Credentials
    Given Featureform is installed

    When I create the GCP credentials with a project id "<project_id>" and credentials path "<credentials_path>"
    Then An exception that "matches" "<exception>" should be raised
    Examples:
      | project_id | credentials_path | exception |
      | my-project | test_files/dummy_creds.json | None  |
      | empty  | test_files/dummy_creds.json | 'GCPCredentials' project_id cannot be empty  |
      |  my-project | empty | 'GCPCredentials' credentials_path cannot be empty or credentials cannot be None  |
      |  my-project | invalid/file.json | 'GCPCredentials' credentials_path 'invalid/file.json' file not found |
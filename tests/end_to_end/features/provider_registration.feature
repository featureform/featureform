Feature: Provider Registration

  @wip
  Scenario Outline: S3 Registration
    Given Featureform is installed
    When I create a "hosted" "insecure" client for "localhost:7878"
    And I create dummy "<cloud_provider>" credentials
    And I register S3 with name "<name>", bucket_name "<bucket_name>", bucket_region "<bucket_region>", path "<path>"
    Then An exception that "matches" "<exception>" should be raised
    Examples:
      | name | cloud_provider | bucket_name | bucket_region | path | exception |
      | test |       AWS      |     test    |    us-east-1  | test |   None    |
      | test |       GCP      |     test    |    us-east-1  | test |   type of argument "credentials" must be featureform.providers.credentials.AWSCredentials; got featureform.providers.credentials.GCPCredentials    |

  @wip
  Scenario Outline: GCS Registration
    Given Featureform is installed
    When I create a "hosted" "insecure" client for "localhost:7878"
    And I create dummy "<cloud_provider>" credentials
    And I register GCS with name "<name>", bucket_name "<bucket_name>", path "<path>"
    Then An exception that "matches" "<exception>" should be raised

    Examples:
      | name | cloud_provider | bucket_name  | path | exception |
      | test |       AWS      |     test     | test |   type of argument "credentials" must be featureform.providers.credentials.GCPCredentials; got featureform.providers.credentials.AWSCredentials instead    |
      | test |       GCP      |     test     | test |   None    |
  
  Scenario Outline: Offline Provider Registration and Get Provider
    Given Featureform is installed
    When I create a "hosted" "insecure" client for "localhost:7878"
    And I register a "<offline_provider>"
    Then I can get the offline provider
    And I can register a dataset located at "<dataset_path>"
    And I can get the pandas dataframe of the dataset

    Examples:
      | offline_provider | dataset_path |
      |     Snowflake    | transactions |
      |     Postgres     | Transactions |
  
  Scenario Outline: Online Provider Registration and Get Provider
    Given Featureform is installed
    When I create a "hosted" "insecure" client for "localhost:7878"
    And I register a "<offline_provider>"
    And I register a "<online_provider>"
    Then I can get the online provider
    And I can register a dataset located at "<dataset_path>"
    And I can register a feature based on the column "<column_name>" with "<feature_type>"
    And I can serve the feature

    Examples:
      | offline_provider | online_provider | dataset_path | column_name  | feature_type |
      |     Postgres     |      redis      | transactions |    Amount    |   float64    |
      |     Postgres     |     dynamoDB    | Transactions |    Amount    |   float64    |
    
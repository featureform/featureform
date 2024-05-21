Feature: Provider Registration

  @wip
  Scenario Outline: S3 Registration
    Given Featureform is installed
    When I create a "hosted" "insecure" client for "localhost:7878"
    And I create dummy "<cloud_provider>" credentials
    And I register S3 with name "<name>", bucket_name "<bucket_name>", bucket_region "<bucket_region>", path "<path>"
    Then An exception that "matches" "<exception>" should be raised
    Examples:
      | name | cloud_provider | bucket_name | bucket_region | path | exception                                                                                                                                     |
      | test | AWS            | test        | us-east-1     | test | None                                                                                                                                          |
      | test | GCP            | test        | us-east-1     | test | type of argument "credentials" must be featureform.providers.credentials.AWSCredentials; got featureform.providers.credentials.GCPCredentials |

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

  Scenario Outline: Registering a provider and get provider to register a source
    Given Featureform is installed
    When I turn on autovariants
    And I create a "hosted" "insecure" client for "localhost:7878"
    And I register an offline provider of type "<offline_provider_type>"
    # And I can get the offline provider
    And I register an online provider of type "<online_provider_type>"
    # And I can get the online provider
    And I register a dataset located at "<dataset_path>"
    And I register a feature on "<feature_column>" with type "<feature_type>" with "<entity_column>", "<timestamp_column>", and "<label_column>"
    Then I can register a training set based on the feature
    And I can serve the registered feature with the model for "<user>" with "<expected_value>"
    And I can serve the registered training set with the model
    And I turn off autovariants

    Examples:
      | online_provider_type | offline_provider_type | dataset_path | feature_column    | feature_type | entity_column | timestamp_column | label_column | user     | expected_value |
      | redis                | postgres              | transactions | transactionamount | float64      | customerid    | empty            | isfraud      | C5841053 | 25.0           |

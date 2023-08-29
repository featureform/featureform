Feature: Provider Registration

  Scenario Outline: S3 Registration
    Given Featureform is installed
    When I create a "hosted" "insecure" client for "localhost:7878"
    And I create dummy "<cloud_provider>" credentials
    And I register S3 with name "<name>", bucket_name "<bucket_name>", bucket_region "<bucket_region>", path "<path>"
    Then An exception "<exception>" should be raised
    Examples:
      | name | cloud_provider | bucket_name | bucket_region | path | exception |
      | test |       AWS      |     test    |    us-east-1  | test |   None    |
      | test |       GCP      |     test    |    us-east-1  | test |   type of argument "credentials" must be featureform.resources.AWSCredentials; got featureform.resources.GCPCredentials instead    |


  Scenario Outline: GCS Registration
    Given Featureform is installed
    When I create a "hosted" "insecure" client for "localhost:7878"
    And I create dummy "<cloud_provider>" credentials
    And I register GCS with name "<name>", bucket_name "<bucket_name>", path "<path>"
    Then An exception "<exception>" should be raised

    Examples:
      | name | cloud_provider | bucket_name  | path | exception |
      | test |       AWS      |     test     | test |   type of argument "credentials" must be featureform.resources.GCPCredentials; got featureform.resources.AWSCredentials instead    |
      | test |       GCP      |     test     | test |   None    |


  Scenario Outline: Spark Registration
    Given Featureform is installed
    When I create a "hosted" "insecure" client for "localhost:7878"
    And I create Generic Spark credentials with master "<master>", deploy mode "<deploy_mode>", python version "<python_version>", core site path "<core_site_path>", yarn site path "<yarn_site_path>"
    And I create the AWS credentials with an access key "<access_key>" and secret key "<secret_key>"
#    And I register spark
#    And I fetch the spark registration
#    Then I should see Generic Spark credentials with master "<expected_master>", deploy mode "<expected_deploy_mode>", python version "<expected_python_version>", core site path "<expected_core_site_path>", yarn site path "<expected_yarn_site_path>"
#    And I should see AWS credentials with an access key "<expected_access_key>" and secret key "<expected_secret_key>"
    Then An exception "<exception>" should be raised
    Examples:
      | master | deploy_mode | python_version | core_site_path | yarn_site_path | access_key | secret_key | expected_master | expected_deploy_mode | expected_python_version | expected_core_site_path | expected_yarn_site_path | expected_access_key | expected_secret_key | exception |
      | local  |   client    |      3.7       |     empty      |      empty     |     ABC    |     123    |      local      |        client        |           3.7           |          empty          |          empty          |         ABC         |         123         |    None   |


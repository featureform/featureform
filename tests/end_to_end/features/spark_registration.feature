Feature: Spark Registration

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

#  Scenario Outline: S3 Credential Rotation
#    Given Featureform is installed
#    When I create the AWS credentials with an access key <access_key> and secret key <secret_key>
#    When I register S3 with name <name>, credentials <credentials>, bucket_path <bucket_path>, bucket_region <bucket_region>, path <path>
#    When I register S3 with name <name>, credentials <credentials>, bucket_path <bucket_path>, bucket_region <bucket_region>, path <path>
#    Then I should see the S3 registration with name <exp_name>, credentials <exp_credentials>, bucket_path <exp_bucket_path>, bucket_region <exp_bucket_region>, path <exp_path>
#    Examples:
#      | access_key | secret_key | name | credentials | bucket_path | bucket_region | path | exp_name | exp_credentials | exp_bucket_path | exp_bucket_region | exp_path |
#
#  Scenario Outline: Blob Store Credential Rotation
#    Given Featureform is installed
#    When I register Blob Store with name <name>, account_name <account_name>, account_key <account_key>, container_name <container_name>, root_path <root_path>
#    Then I should see the Blob Store registration with name <exp_name>, account_name <exp_account_name>, account_key <exp_account_key>, container_name <exp_container_name>, root_path <exp_root_path>
#    Examples:
#      | name | account_name | account_key | container_name | root_path | exp_name | exp_account_name | exp_account_key | exp_container_name | exp_root_path |
#
#  Scenario Outline:  GCS Credentials Rotation
#    Given Featureform is installed
#    When I create the GCP credentials with an project id <project_id> and credentials path <credentials_path>
#    When I register GCS with name <name>, credentials <credentials>, bucket_path <bucket_path>, path <path>
#    Then I should see the GCS registration with name <exp_name>, credentials <exp_credentials>, bucket_path <exp_bucket_path>, path <exp_path>
#    Examples:
#      | project_id | credentials_path | name | credentials | bucket_path | path | exp_name | exp_credentials | exp_bucket_path | exp_path |
#
#  Scenario Outline:  EMR Credential Rotation
#    Given Featureform is installed
#    When I create valid AWS credentials
#    And I register EMR with Cluster ID <emr_cluster_id>, Region <emr_cluster_region>, credentials <credentials>
#    Then I should see the EMR registration with Cluster ID <exp_emr_cluster_id>, Region <exp_emr_cluster_region>, credentials <exp_credentials>
#    Examples:
#      | emr_cluster_id | emr_cluster_region | credentials | exp_emr_cluster_id | exp_emr_cluster_region | exp_credentials |
#
#  Scenario Outline:  Databricks Credentials Rotation
#    Given Featureform is installed
#    When I register Databricks with Username <username>, password <password>, host <host>, token <token>, cluster ID <cluster_id>
#    Then I should see the Databricks registration with Username <exp_username>, password <exp_password>, host <exp_host>, token <exp_token>, cluster ID <exp_cluster_id>
#    Examples:
#      | username | password | host | token | cluster_id | exp_username | exp_password | exp_host | exp_token | exp_cluster_id |
#
#  Scenario Outline:  Generic Spark Credentials Rotation
#    Given Featureform is installed
#    When I register Generic Spark with master <master>, deploy mode <deploy_mode>, python version <python_version>, core site path <core_site_path>, yarn site path <yarn_site_path>
#
#    Examples:
#      |  | isValid |
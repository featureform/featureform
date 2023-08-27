Feature: Spark Credentials

  Scenario Outline: EMR Credentials
    Given Featureform is installed
    When I create dummy "<cloud_provider>" credentials
    And I create EMR credentials with Cluster ID "<emr_cluster_id>", Region "<emr_cluster_region>"
    Then An exception "<exception>" should be raised
    Examples:
      | emr_cluster_id        | emr_cluster_region         | cloud_provider | exception     |
      |       DUMMYID         |         us-east-1          |      AWS       |     None      |
      |       empty           |         us-east-1          |      AWS       |     'EMRCredentials' emr_cluster_id cannot be empty      |
      |       DUMMYID         |             empty          |      AWS       |     'EMRCredentials' emr_cluster_region cannot be empty      |
      |       DUMMYID         |         us-east-1          |      empty     |     type of argument "credentials" must be featureform.resources.AWSCredentials; got NoneType instead     |
      |       DUMMYID         |         us-east-1          |      GCP       |     type of argument "credentials" must be featureform.resources.AWSCredentials; got featureform.resources.GCPCredentials instead     |

  Scenario Outline: Databricks Credentials
    Given Featureform is installed
    When I create Databricks credentials with username "<username>", password "<password>", host "<host>", token "<token>", cluster ID "<cluster_id>"
    Then An exception "<exception>" should be raised
    Examples:
      | username | password | host | token | cluster_id | exception |
      |   dummy  |   dummy  | empty| empty |   dummy    |   None   |
      |   empty  |   empty  | dummy| dummy |   dummy    |   None   |
      |   dummy  |   dummy  | dummy| dummy |   dummy    |   'DatabricksCredentials' requires either 'username' and 'password' or 'host' and 'token' to be set    |
      |   empty  |   empty  | empty| empty |   empty    |   'DatabricksCredentials' requires either 'username' and 'password' or 'host' and 'token' to be set   |
      |   dummy  |   empty  | empty| empty |   empty    |   'DatabricksCredentials' requires either 'username' and 'password' or 'host' and 'token' to be set   |
      |   empty  |   dummy  | empty| empty |   empty    |   'DatabricksCredentials' requires either 'username' and 'password' or 'host' and 'token' to be set   |
      |   empty  |   empty  | dummy| empty |   empty    |   'DatabricksCredentials' requires either 'username' and 'password' or 'host' and 'token' to be set   |
      |   empty  |   empty  | empty| dummy |   empty    |   'DatabricksCredentials' requires either 'username' and 'password' or 'host' and 'token' to be set   |
      |   empty  |   empty  | empty| empty |   dummy    |   'DatabricksCredentials' requires either 'username' and 'password' or 'host' and 'token' to be set   |
      |   dummy  |   dummy  | empty| empty |   empty    |   Cluster_id of existing cluster must be provided   |
      |   empty  |   empty  | dummy| dummy |   empty    |   Cluster_id of existing cluster must be provided   |

  Scenario Outline: Generic Credentials
    Given Featureform is installed
    When I create Generic Spark credentials with master "<master>", deploy mode "<deploy_mode>", python version "<python_version>", core site path "<core_site_path>", yarn site path "<yarn_site_path>"
    Then An exception "<exception>" should be raised
    Examples: Versions
      | master | deploy_mode | python_version | core_site_path | yarn_site_path | exception |
      |  local[*] |     Client   |      3.6     |      empty     |     empty       |   The Python version 3.6 is not supported. Currently, supported versions are 3.7-3.11.    |
      |  local[*] |     Client   |      3.7     |      empty     |     empty       |   None    |
      |  local[*] |     Client   |      3.8     |      empty     |     empty       |   None    |
      |  local[*] |     Client   |      3.9     |      empty     |     empty       |   None    |
      |  local[*] |     Client   |      3.10    |      empty     |     empty       |   None    |
      |  local[*] |     Client   |      3.11    |      empty     |     empty       |   None    |
      |  local[*] |     dummy    |      3.7     |      empty     |     empty       |   Spark does not support 'dummy' deploy mode. It only supports 'cluster' and 'client'.    |

    Examples: Yarn
      | master | deploy_mode | python_version | core_site_path | yarn_site_path | exception |
      |  yarn     |     Client   |      3.7     |      test_files/dummy_core_site.xml     |     test_files/dummy_yarn_site.xml       |   None   |
      |  yarn     |     Cluster  |      3.7     |      test_files/dummy_core_site.xml     |     test_files/dummy_yarn_site.xml       |   None   |
      |  yarn     |     Client   |      3.7     |      empty     |     test_files/dummy_yarn_site.xml       |   Yarn requires core-site.xml and yarn-site.xml files. Please copy these files from your Spark instance to local, then provide the local path in core_site_path and yarn_site_path.   |
      |  yarn     |     Client   |      3.7     |      test_files/dummy_core_site.xml     |     empty       |   Yarn requires core-site.xml and yarn-site.xml files. Please copy these files from your Spark instance to local, then provide the local path in core_site_path and yarn_site_path.   |
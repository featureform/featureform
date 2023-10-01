Feature: Provider Updates

  @wip
  Scenario Outline: Rotate S3 Credentials
    Given Featureform is installed
    When I create a "hosted" "insecure" client for "localhost:7878"
    And I create the AWS credentials with an access key "<access_key>" and secret key "<secret_key>"
    And I register S3 with name "<name>", bucket_name "<bucket_name>", bucket_region "<bucket_region>", path "<path>"
    And I create the AWS credentials with an access key "<repl_access_key>" and secret key "<repl_secret_key>"
    And I register S3 with name "<repl_name>", bucket_name "<repl_bucket_path>", bucket_region "<repl_region>", path "<repl_path>"
    And I get the provider with name "<name>"
    Then the S3 provider should have bucket_name "<exp_bucket_name>", bucket_region "<exp_bucket_region>", path "<exp_path>", access key "<exp_access_key>" and secret key "<exp_secret_key>"
    And An exception that "matches" "<exception>" should be raised

    Examples: Base Case
      | name  | bucket_name | bucket_region | path    | access_key   | secret_key   | repl_name  | repl_bucket_path | repl_path   | repl_region  | repl_access_key   | repl_secret_key   | exp_bucket_name | exp_bucket_region | exp_path  | exp_access_key   |  exp_secret_key  |
      | s3_1    |     bucket  |    us-east-1  | my/path |  access_key  |  secret_key  |   test   |   bucket         |   my/path   |   us-east-1  |    access_key     |    secret_key     |    bucket       |     us-east-1     |  my/path  |    access_key    |    secret_key    |

    Examples: Rotate Credentials
      | name  | bucket_name | bucket_region | path    | access_key   | secret_key   | repl_name |repl_bucket_path | repl_path   | repl_region  | repl_access_key   | repl_secret_key   | exp_bucket_name | exp_bucket_region | exp_path  | exp_access_key   |  exp_secret_key   |
      | s3_2    |     bucket  |    us-east-1  | my/path |  access_key  |  secret_key  |   test  |     bucket      |    my/path   |   us-east-1  |    access_key     |    secret_key     |    bucket       |     us-east-1     |  my/path  |    access_key    |    secret_key    |
      | s3_2    |     bucket  |    us-east-1  | my/path |  access_key  |  secret_key  |   test  |     bucket      |    my/path   |   us-east-1  |    access_key2    |    secret_key     |    bucket       |     us-east-1     |  my/path  |    access_key2   |    secret_key    |
      | s3_2    |     bucket  |    us-east-1  | my/path |  access_key  |  secret_key  |   test  |     bucket      |    my/path   |   us-east-1  |    access_key     |    secret_key2    |    bucket       |     us-east-1     |  my/path  |    access_key    |    secret_key2   |
      | s3_2    |     bucket  |    us-east-1  | my/path |  access_key  |  secret_key  |   test  |     bucket      |    my/path   |   us-east-1  |    access_key2    |    secret_key2    |    bucket       |     us-east-1     |  my/path  |    access_key2   |    secret_key2   |

    Examples: Change Path
      | name    | bucket_name | bucket_region |  path    | access_key   | secret_key   | repl_name| repl_bucket_path  |        repl_path       | repl_region  | repl_access_key   | repl_secret_key   | exp_bucket_name | exp_bucket_region | exp_path  | exp_access_key   |  exp_secret_key  |
      | s3_3    |    bucket  |    us-east-1  | my/path  |  access_key   |  secret_key  |   test   |        bucket     |  my/different/path   |   us-east-1  |    access_key     |    secret_key     |    bucket       |     us-east-1     |  my/path  |    access_key    |    secret_key    |

    Examples: Change Bucket
      | name    | bucket_name | bucket_region |  path    | access_key   | secret_key   | repl_name | repl_bucket_path  |        repl_path       | repl_region  | repl_access_key   | repl_secret_key   | exp_bucket_name | exp_bucket_region | exp_path  | exp_access_key   |  exp_secret_key  |
      | s3_3    |    bucket   |    us-east-1  | my/path  |  access_key  |  secret_key  |   test    |     bucket        |   my/different/path    |   us-east-1  |    access_key     |    secret_key     |    bucket       |     us-east-1     |  my/path  |    access_key    |    secret_key    |

    Examples: Change Region
      | name    | bucket_name | bucket_region | path    | access_key   | secret_key   | repl_name  | repl_bucket_path | repl_path   | repl_region  | repl_access_key   | repl_secret_key   | exp_bucket_name | exp_bucket_region | exp_path  | exp_access_key   |  exp_secret_key  |
      | s3_1    |     bucket  |    us-east-1  | my/path |  access_key  |  secret_key  |   test     |   bucket         |   my/path   |   us-east-2  |    access_key     |    secret_key     |    bucket       |     us-east-1     |  my/path  |    access_key    |    secret_key    |


  @wip
  Scenario Outline: Azure Blob Store Registration
    Given Featureform is installed
    When I create a "hosted" "insecure" client for "localhost:7878"
    And I register Blob Store with name "<name>", account_name "<account_name>", account_key "<account_key>", container_name "<container_name>", path "<path>"
    And I register Blob Store with name "<name>", account_name "<repl_account_name>", account_key "<repl_account_key>", container_name "<repl_container_name>", path "<repl_path>"
    And I get the provider with name "<name>"
    Then The blob store provider with name <name> should have account_name "<exp_account_name>", account_key "<exp_account_key>", container_name "<exp_container_name>", path "<exp_path>"
    And An exception that "matches" "<exception>" should be raised
    Examples: Base Case
      | name | account_name | account_key | container_name | path | repl_account_name | repl_account_key | repl_container_name | repl_path | exp_account_name | exp_account_key | exp_container_name | exp_path | exception |
      | blob | account_name | account_key | container_name | path |  account_name     |  account_key     |  container_name     | path      | account_name     | account_key     | container_name     | path     | None      |

    Examples: Change Account Name
      | name  | account_name | account_key | container_name | path | repl_account_name | repl_account_key | repl_container_name | repl_path | exp_account_name | exp_account_key | exp_container_name | exp_path | exception |
      | blob2 | account_name | account_key | container_name | path |  account_name2     |  account_key     |  container_name     | path      | account_name     | account_key     | container_name     | path     | None      |

    Examples: Change Account Key
      | name  | account_name | account_key | container_name | path | repl_account_name | repl_account_key | repl_container_name | repl_path | exp_account_name | exp_account_key | exp_container_name | exp_path | exception |
      | blob2 | account_name | account_key | container_name | path |  account_name     |  account_key2     |  container_name     | path      | account_name     | account_key2     | container_name     | path     | None      |

    Examples: Change Container Name
      | name  | account_name | account_key | container_name | path | repl_account_name | repl_account_key | repl_container_name | repl_path | exp_account_name | exp_account_key | exp_container_name | exp_path | exception |
      | blob2 | account_name | account_key | container_name | path |  account_name     |  account_key     |  container_name2     | path      | account_name     | account_key     | container_name    | path     | None      |

    Examples: Change Container Path
      | name  | account_name | account_key | container_name | path | repl_account_name | repl_account_key | repl_container_name | repl_path | exp_account_name | exp_account_key | exp_container_name | exp_path | exception |
      | blob2 | account_name | account_key | container_name | path |  account_name     |  account_key     |  container_name     | path2      | account_name     | account_key     | container_name    | path     | None      |

  @wip
  Scenario Outline: Rotate GCS Credentials
    Given Featureform is installed
    When I create a "hosted" "insecure" client for "localhost:7878"
    And I create the GCP credentials with a project id "<project_id>" and credentials path "<credentials_path>"
    And I register GCS with name "<name>", bucket_name "<bucket_name>", path "<path>"
    And I create the GCP credentials with a project id "<repl_project_id>" and credentials path "<repl_credentials_path>"
    And  I register GCS with name "<name>", bucket_name "<repl_bucket_name>", path "<repl_path>"
    And I get the provider with name "<name>"
    Then the GCS provider should have project id "<exp_project_id>" bucket_name "<exp_bucket_name>", path "<exp_path>", credentials from path "<exp_credentials_path>"

    Examples: Base Case
      | name | project_id  |       credentials_path          | bucket_name    |  path  | repl_project_id    |         repl_credentials_path                |  repl_bucket_name  | repl_path | exp_project_id |  exp_bucket_name  | exp_path | exp_credentials_path |
      | gcs1 |  project    |   test_files/dummy_creds.json   |     bucket     |   /    |      project       |         test_files/dummy_creds.json          |       bucket       |     /     |       project  |   bucket       |    /     | test_files/dummy_creds.json |

    Examples: Change Project ID
      | name | project_id  |       credentials_path          | bucket_name    |  path  | repl_project_id    |         repl_credentials_path                |  repl_bucket_name  | repl_path | exp_project_id |  exp_bucket_name  | exp_path | exp_credentials_path |
      | gcs2 |  project    |   test_files/dummy_creds.json   |     bucket     |   /    |      project2       |         test_files/dummy_creds.json          |       bucket       |     /     |  project      |   bucket       |    /     |test_files/dummy_creds.json |

    Examples: Change Bucket Name
      | name | project_id  |       credentials_path          | bucket_name    |  path  | repl_project_id    |         repl_credentials_path                |  repl_bucket_name  | repl_path | exp_project_id |  exp_bucket_name  | exp_path |exp_credentials_path |
      | gcs3 |  project    |   test_files/dummy_creds.json   |     bucket     |   /    |      project       |         test_files/dummy_creds.json          |       bucket2       |     /     |  project      |   bucket       |    /     | test_files/dummy_creds.json     |

    Examples: Change Path
      | name | project_id  |       credentials_path          | bucket_name    |  path  | repl_project_id    |         repl_credentials_path                |  repl_bucket_name  | repl_path | exp_project_id |  exp_bucket_name  | exp_path | exp_credentials_path |
      | gcs1 |  project    |   test_files/dummy_creds.json   |     bucket     |   /    |      project       |         test_files/dummy_creds.json          |       bucket       |     /diff/path     |       project  |   bucket       |    /     |test_files/dummy_creds.json |

    Examples: Change Credentials
      | name | project_id  |       credentials_path          | bucket_name    |  path  | repl_project_id    |         repl_credentials_path                |  repl_bucket_name  | repl_path | exp_project_id |  exp_bucket_name  | exp_path | exp_credentials_path |
      | gcs1 |  project    |   test_files/dummy_creds.json   |     bucket     |   /    |      project       |         test_files/dummy_creds.json          |       bucket       |     /     |       project  |   bucket       |    /     | test_files/dummy_creds.json |



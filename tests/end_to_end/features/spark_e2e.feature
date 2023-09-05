Feature: Spark End to End

  # When pulling a file from a different type of filestore as the one registered, an error for invalid prefix
  # is thrown and the error is overwritten on retries when trying to pull the file as a dataframe

  # Need an error for if a parent dependency is not registered
  Scenario Outline: Register a file
    Given Featureform is installed
    When I create a "hosted" "insecure" client for "localhost:7878"
    And I generate a random variant name
    And I upload a "<filesize>" "<filetype>" file to "<storage_provider>"
    And I register "<storage_provider>" filestore with bucket "<bucket>" and root path "behave"
    And I register databricks
    And I register the file
    Then I should be able to pull the file as a dataframe
    Examples: Azure
      | filesize | filetype | storage_provider | bucket |
      |  small   |   csv    |       azure      | test   |
      |  small   |  parquet |       azure      | test   |
      |  large   |   csv    |       azure      | test   |
      |  large   |  parquet |       azure      | test   |
#      |  small   | directory|       azure      | test   |  # Fails due to cannot read directory
#      |  large   | directory|       azure      | test   |  # Fails due to cannot read directory
#
    Examples: S3
      | filesize | filetype | storage_provider |          bucket         |
      |  small   |   csv    |        s3        |featureform-spark-testing|
      |  small   |  parquet |        s3        |featureform-spark-testing|
      |  large   |   csv    |        s3        |featureform-spark-testing|
      |  large   |  parquet |        s3        |featureform-spark-testing|
#      |  small   | directory|        s3        |featureform-spark-testing|  # Fails due to cannot read directory
#      |  large   | directory|        s3        |featureform-spark-testing|  # Fails due to cannot read directory

    Examples: GCS
      | filesize | filetype |  storage_provider |     bucket     |
      |  small   |   csv    |        gcs        |featureform-test|
      |  small   |  parquet |        gcs        |featureform-test|
      |  large   |   csv    |        gcs        |featureform-test|
      |  large   |  parquet |        gcs        |featureform-test|
#      |  small   | directory|        gcs        |featureform-test|  # Fails due to cannot read directory
#      |  large   | directory|        gcs        |featureform-test|  # Fails due to cannot read directory

  Scenario Outline: Register a file with invalid stores
    Given Featureform is installed
    When I create a "hosted" "insecure" client for "localhost:7878"
    And I generate a random variant name
    And I upload a "small" "csv" file to "<storage_provider>"
    And I register "<storage_provider>" filestore with bucket "<bucket>" and root path "<path>"
    And I register databricks
    And I register the file
    Then I should be able to pull the file as a dataframe
    Then An exception that "matches" "<exception>" should be raised


    Examples: Base Case
      | storage_provider |          bucket         |  path  |        exception         |
#      |       azure      |           test          |  test  |  None  |
#      |        s3        |featureform-spark-testing|  test  |  None  |
#      |        gcs       |    featureform-test     |  test  |  None  |

    Examples: Invalid Bucket
      | storage_provider |          bucket         |  path  |        exception         |
#      |       azure      |         invalid         |  test  |          None            |
#      |        s3        |         invalid         |  test  |          None            |
#      |        gcs       |         invalid         |  test  |          None            |


    # The current error is not useful
    Examples: Invalid Path
      | storage_provider |          bucket         |  path  |        exception         |
      |       azure      |           test          |  /  |  None  |
      |       azure      |           test          |  empty  |  None  |
      |        s3        |featureform-spark-testing|  /  |  None  |
      |        s3        |featureform-spark-testing|  empty  |  None  |
      |        gcs       |    featureform-test     |  /  |  None  |
      |        gcs       |    featureform-test     |  empty  |  None  |



#  Scenario Outline: Databricks End to End
#      Given Featureform is installed
#      When I create a "hosted" "insecure" client for "localhost:7878"
#      And I generate a random variant name
#      And I upload a "<filesize>" "<filetype>" file to "<storage_provider>"
#      And I register redis
#      And I register databricks
#      And I register the file
#      Then I should be able to pull the file as a dataframe
#      When I register a transformation
#      Then I should be able to pull the transformation as a dataframe
#      When I register a feature
#      Then I should be able to fetch a feature as a dataframe
#      When I register a label
#      And I register a training set
#      Then I should be able to pull the trainingset as a dataframe
#
#      Examples: File Sizes
#      | filesize | filetype | storage_provider |
#      |  small   |   csv    |       azure      |
#      |  small   |  parquet |       azure      |
#      |  large   |   csv    |       azure      |
#      |  large   |  parquet |       azure      |
#      |  small   |  directory |       azure      |

#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

Feature: Spark End to End

  # When pulling a file from a different type of filestore as the one registered, an error for invalid prefix
  # is thrown and the error is overwritten on retries when trying to pull the file as a dataframe

  # Need an error for if a parent dependency is not registered

  Scenario Outline: Register a small file in spark
    Given Featureform is installed
    When I create a "hosted" "insecure" client for "localhost:7878"
    And I generate a random variant name
    And I upload a "small" "<filetype>" file to "<storage_provider>"
    And I register "<storage_provider>" filestore with bucket "<bucket>" and root path "behave"
    And I get or register databricks
    And I register the file
    Then I should be able to pull the file as a dataframe
    Examples: Azure
      | filetype | storage_provider | bucket |
    #      | csv       | azure            | test   |
    #      | parquet   | azure            | test   |
    #      | directory | azure            | test   |

    Examples: S3
      | filetype  | storage_provider | bucket                    |
      | csv       | s3               | ff-spark-testing |
      | parquet   | s3               | ff-spark-testing |
      | directory | s3               | ff-spark-testing |

    Examples: GCS
      | filetype | storage_provider | bucket |
  #      |   csv    |        gcs        |featureform-test| # TODO: Enable Later
  #      |  parquet |        gcs        |featureform-test| # TODO: Enable Later
  #      | directory|        gcs        |featureform-test| # TODO: Enable Later

  Scenario Outline: Register and Get Spark
    Given Featureform is installed
    When I create a "hosted" "insecure" client for "localhost:7878"
    And I generate a random variant name
    And I upload a "small" "<filetype>" file to "<storage_provider>"
    And I register "<storage_provider>" filestore with bucket "<bucket>" and root path "behave"
    And I get or register databricks
    And I get spark
    And I register the file
    Then I should be able to pull the file as a dataframe
    Examples: Azure
      | filetype | storage_provider | bucket |
    #      |   csv    |       azure      | test   |

    Examples: S3
      | filetype | storage_provider | bucket                    |
      | csv      | s3               | ff-spark-testing |

  @long @wip
  Scenario Outline: Register a large file in spark
    Given Featureform is installed
    When I create a "hosted" "insecure" client for "localhost:7878"
    And I generate a random variant name
    And I upload a "large" "<filetype>" file to "<storage_provider>"
    And I register "<storage_provider>" filestore with bucket "<bucket>" and root path "behave"
    And I get or register databricks
    And I register the file
    Then I should be able to pull the file as a dataframe
    Examples: Azure
      | filetype | storage_provider | bucket |
    #      | csv       | azure            | test   |
    #      | parquet   | azure            | test   |
    #      | directory | azure            | test   |

    Examples: S3
      | filetype  | storage_provider | bucket                    |
      | csv       | s3               | ff-spark-testing |
      | parquet   | s3               | ff-spark-testing |
      | directory | s3               | ff-spark-testing |

    Examples: GCS
      | filetype | storage_provider | bucket |
  #      |   csv    |        gcs        |featureform-test|
  #      |  parquet |        gcs        |featureform-test|
  #      | directory|        gcs        |featureform-test|

  @wip
  Scenario Outline: Register a file with invalid stores
    Given Featureform is installed
    When I create a "hosted" "insecure" client for "localhost:7878"
    And I generate a random variant name
    And I upload a "small" "csv" file to "<storage_provider>"
    And I register "<storage_provider>" filestore with bucket "<bucket>" and root path "<path>"
    And I get or register databricks
    And I register the file
    Then I should be able to pull the file as a dataframe
    Then An exception that "matches" "<exception>" should be raised


    Examples: Base Case
      | storage_provider | bucket                    | path | exception |
      | azure            | test                      | test | None      |
      | s3               | ff-spark-testing | test | None      |
      | gcs              | featureform-test          | test | None      |

    Examples: Invalid Bucket
      | storage_provider | bucket  | path | exception |
      | azure            | invalid | test | None      |
      | s3               | invalid | test | None      |
      | gcs              | invalid | test | None      |


    # The current error is not useful
    Examples: Invalid Path
      | storage_provider | bucket                    | path  | exception |
      | azure            | test                      | /     | None      |
      | azure            | test                      | empty | None      |
      | s3               | ff-spark-testing | /     | None      |
      | s3               | ff-spark-testing | empty | None      |
      | gcs              | featureform-test          | /     | None      |
      | gcs              | featureform-test          | empty | None      |


  Scenario Outline: Databricks End to End
    Given Featureform is installed
    When I create a "hosted" "insecure" client for "localhost:7878"
    And I generate a random variant name
    And I upload a "<filesize>" "<filetype>" file to "<storage_provider>"
    And I get or register redis
    And I register "<storage_provider>" filestore with bucket "<bucket>" and root path "behave"
    And I get or register databricks
    And I register the file
    Then I should be able to pull the file as a dataframe
    When I register a "DF" transformation named "first_transformation" from "transactions"
    Then I should be able to pull the transformation as a dataframe
    When I register a feature from a "<feature_source>"
    When I register a label from a "<label_source>"
    And I register a training set
    Then I should be able to pull the trainingset as a dataframe

    Examples: Azure
      | filesize | filetype | storage_provider | bucket | feature_source | label_source |
    #      | small    | csv      | azure            | test   | transformation | transformation |
    #      | small    | csv      | azure            | test   | primary        | primary        |
    #      | small    | csv      | azure            | test   | transformation | primary        |
    #      | small    | csv      | azure            | test   | primary        | transformation |
    #      |  small   |   parquet    |       azure      | test   | transformation | transformation | # TODO: Enable
    #      |  small   |   parquet    |       azure      | test   |    primary     |     primary    | # TODO: Enable
    #      |  small   |   parquet    |       azure      | test   | transformation |     primary    | # TODO: Enable
    #      |  small   |   parquet    |       azure      | test   |    primary     | transformation | # TODO: Enable


    Examples: S3
      | filesize | filetype | storage_provider | bucket                    | feature_source | label_source   |
      | small    | csv      | s3               | ff-spark-testing | transformation | transformation |
      | small    | csv      | s3               | ff-spark-testing | primary        | primary        |
      | small    | csv      | s3               | ff-spark-testing | transformation | primary        |
      | small    | csv      | s3               | ff-spark-testing | primary        | transformation |
    #      |  small   |   parquet    |         s3       | test   | transformation | transformation | # TODO: Enable
    #      |  small   |   parquet    |         s3       | test   |    primary     |     primary    | # TODO: Enable
    #      |  small   |   parquet    |         s3       | test   | transformation |     primary    | # TODO: Enable
    #      |  small   |   parquet    |         s3       | test   |    primary     | transformation | # TODO: Enable

    Examples: GCS
      | filesize | filetype | storage_provider | bucket | feature_source | label_source |
  #      |  small   |      csv     |        gcs       | test   | transformation | transformation | # Broken
  #      |  small   |      csv     |        gcs       | test   |    primary     |     primary    | # Broken
  #      |  small   |      csv     |        gcs       | test   | transformation |     primary    | # Broken
  #      |  small   |      csv     |        gcs       | test   |    primary     | transformation | # Broken
  #      |  small   |   parquet    |        gcs       | test   | transformation | transformation | # TODO: Enable
  #      |  small   |   parquet    |        gcs       | test   |    primary     |     primary    | # TODO: Enable
  #      |  small   |   parquet    |        gcs       | test   | transformation |     primary    | # TODO: Enable
  #      |  small   |   parquet    |        gcs       | test   |    primary     | transformation | # TODO: Enable

  @av
  Scenario Outline: Iceberg Table Registration
    Given Featureform is installed
    When I create a "hosted" "insecure" client for "localhost:7878"
    And I get or register redis
    And I register "s3" filestore with bucket "ff-spark-testing" and root path "empty"
    And I get or register EMR with glue
    And I register an iceberg table
    When I register a "<transformation_type>" transformation named "<transformation_name>" from "primary" (v2)
    Then I should be able to pull the iceberg transformation as a dataframe
    When I register a feature from a "<feature_source>" (v2)
    When I register "<label_name>" label from a "<label_source>" (v2)
    And I register "<training_set_name>" training set (v2)
    Then I should be able to pull the trainingset as a dataframe (v2)

    Examples: S3
      | transformation_type | transformation_name | feature_source | label_name  | label_source   | training_set_name |
      | SQL                 | t2_transformation   | transformation | fraudulent1 | transformation | fraud_t2          |
      | DF                  | p2_transformation   | primary        | fraudulent2 | primary        | fraud_p2          |
      | SQL                 | tp_transformation   | transformation | fraudulent3 | primary        | fraud_tp          |
      | DF                  | pt_transformation   | primary        | fraudulent4 | transformation | fraud_pt          |

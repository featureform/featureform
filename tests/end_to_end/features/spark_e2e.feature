Feature: Spark End to End

    Scenario Outline: Databricks End to End
      Given Featureform is installed
      When I create a "hosted" "insecure" client for "localhost:7878"
      And I generate a random variant name
      And I upload a "<filesize>" "<filetype>" file to "<storage_provider>"
      And I register redis
      And I register databricks
      And I register the file
      Then I should be able to pull the file as a dataframe
      When I register a transformation
      Then I should be able to pull the transformation as a dataframe
      When I register a feature
#      Then I should be able to pull the feature as a dataframe
      When I register a label
      And I register a training set
      Then I should be able to pull the trainingset as a dataframe

      Examples: File Sizes
      | filesize | filetype | storage_provider |
      |  small   |   csv    |       azure      |
      |  small   |  parquet |       azure      |
#      |  large   |   csv    |       azure      |
#      |  large   |  parquet |       azure      |

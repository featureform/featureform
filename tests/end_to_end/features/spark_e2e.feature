Feature: Spark End to End

    Scenario: Databricks End to End
      Given Featureform is installed
      When I create a "hosted" "insecure" client for "localhost:7878"
      And I upload a file to blob store
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

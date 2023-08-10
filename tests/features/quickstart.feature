Feature: testing localmode quickstart

  Scenario: run localmode quickstart end to end
    Given we have featureform installed
      And we have the transactions file downloaded
      When we create a localmode client
        And we register localmode
        And we register a local file "transactions" at path "transactions.csv"
        And we register a transformation
        And we register a feature
        And we register a label
        And we register a training set
      Then we should be able to serve a feature
      Then we should be able to serve a training set
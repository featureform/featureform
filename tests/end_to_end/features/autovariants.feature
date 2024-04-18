Feature: AutoVariants
    Scenario: Same auto variant for same transformation 
        Given Featureform is installed
        When I create a "hosted" "insecure" client for "localhost:7878"
        And I register "s3" filestore with bucket "featureform-spark-testing" and root path "data"
        And I register databricks
        And I register the file
        And I register a transformation with auto variant
        Then I should be able to reuse the same variant for the same transformation
        And I can get the transformation as df

    Scenario: Same user-provided variant for same transformation
        Given Featureform is installed
        When I create a "hosted" "insecure" client for "localhost:7878"
        And I register "s3" filestore with bucket "featureform-spark-testing" and root path "data"
        And I register databricks
        And I register the file
        And I register a transformation with user-provided variant
        Then I should be able to reuse the same variant for the same transformation
        And I can get the transformation as df
    
    Scenario: Different auto variant for different transformation
        Given Featureform is installed
        When I create a "hosted" "insecure" client for "localhost:7878"
        And I register "s3" filestore with bucket "featureform-spark-testing" and root path "data"
        And I register databricks
        When I register the file
        And I register a transformation with auto variant
        Then I should be able to register a modified transformation with new auto variant
        And I can get the transformation as df

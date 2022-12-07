from featureform.resources import Feature, Label, Source, TrainingSet, Entity, Model, Provider, User, ResourceStatus, ResourceColumnMapping, PrimaryData, SQLTable, Location, LocalConfig

print_outputs = {
    "feature": {
        "unit_test": {
            "object":Feature(
        name="test_name",
        variant="test_variant",
        owner="test_owner",
        value_type="float32",
        description="test_description",
        entity="test_entity",
        provider="test_provider",
        status=ResourceStatus.CREATED,
        location=ResourceColumnMapping(entity="",value="",timestamp=""),
        source=("test_source","test_variant"),
        trainingsets=[("test_training_set","test_variant")]
    ),
            "output":"""NAME:                          test_name
VARIANT:                       test_variant
TYPE:                          float32
ENTITY:                        test_entity
OWNER:                         test_owner
DESCRIPTION:                   test_description
PROVIDER:                      test_provider
STATUS:                        CREATED
-----------------------------------------------
SOURCE:
NAME                           VARIANT
test_source                    test_variant
-----------------------------------------------
TRAINING SETS:
NAME                           VARIANT
test_training_set              test_variant
-----------------------------------------------
"""

        },
        "e2e_test": {
            "object":{
                "output":"""NAME:                          avg_transactions
                              STATUS:                        CREATED
                              -----------------------------------------------
                              VARIANTS:
                              quickstart                     default
                              -----------------------------------------------\n
                          """,
                "return":"""name: "avg_transactions"
                              default_variant: "quickstart"
                              variants: "quickstart"
                              """
            },
            "variant":{
                "output":"""NAME:                          avg_transactions
                                VARIANT:                       quickstart
                                TYPE:                          float32
                                ENTITY:                        user
                                OWNER:                         featureformer
                                PROVIDER:                      redis-quickstart
                                STATUS:                        CREATED
                                -----------------------------------------------
                                SOURCE:
                                NAME                           VARIANT
                                average_user_transaction       quickstart
                                -----------------------------------------------
                                TRAINING SETS:
                                NAME                           VARIANT
                                fraud_training                 quickstart
                                -----------------------------------------------\n
                                """,
            }

        }
    },
    "label": {
        "unit_test": {
            "object":Label(
        name="test_name",
        variant="test_variant",
        value_type="float32",
        owner="test_owner",
        description="test_description",
        entity="test_entity",
        provider="test_provider",
        status=ResourceStatus.CREATED,
        location=ResourceColumnMapping(entity="",value="",timestamp=""),
        source=("test_source","test_variant"),
        trainingsets=[("test_training_set","test_variant")]
    ),
        "output":"""NAME:                          test_name
VARIANT:                       test_variant
TYPE:                          float32
ENTITY:                        test_entity
OWNER:                         test_owner
DESCRIPTION:                   test_description
PROVIDER:                      test_provider
STATUS:                        CREATED
-----------------------------------------------
SOURCE:
NAME                           VARIANT
test_source                    test_variant
-----------------------------------------------
TRAINING SETS:
NAME                           VARIANT
test_training_set              test_variant
-----------------------------------------------
"""

        },
        "e2e_test": {
            "object":{
                "output":"""NAME:                          fraudulent
                            STATUS:                        CREATED
                            -----------------------------------------------
                            VARIANTS:
                            quickstart                     default
                            -----------------------------------------------\n
                          """,
                "return":"""name: "fraudulent"
                              default_variant: "quickstart"
                              variants: "quickstart"
                              """
            },
            "variant":{
                "output":"""NAME:                          fraudulent
                                  VARIANT:                       quickstart
                                  TYPE:                          bool
                                  ENTITY:                        user
                                  OWNER:                         featureformer
                                  PROVIDER:                      postgres-quickstart
                                  STATUS:                        CREATED
                                  -----------------------------------------------
                                  SOURCE:
                                  NAME                           VARIANT
                                  transactions                   kaggle
                                  -----------------------------------------------
                                  TRAINING SETS:
                                  NAME                           VARIANT
                                  fraud_training                 quickstart
                                  -----------------------------------------------\n
                                """

            }

        }
    },
    "trainingset": {
        "unit_test": {
            "object":TrainingSet(
        name="test_name",
        variant="test_variant",
        owner="test_owner",
        description="test_description",
        label=("test_label","test_variant"),
        provider="test_provider",
        status=ResourceStatus.CREATED,
        features=[("test_feature","test_variant")],
        feature_lags=[],
    ),
        "output":"""NAME:                          test_name
VARIANT:                       test_variant
OWNER:                         test_owner
DESCRIPTION:                   test_description
STATUS:                        CREATED
-----------------------------------------------
LABEL:
NAME                           VARIANT
test_label                     test_variant
-----------------------------------------------
FEATURES:
NAME                           VARIANT
test_feature                   test_variant
-----------------------------------------------
"""
        },
        "e2e_test": {
            "object":{
                "output":"""NAME:                          fraud_training
                                  STATUS:                        CREATED
                                  -----------------------------------------------
                                  VARIANTS:
                                  quickstart                     default
                                  -----------------------------------------------\n
                                  """,
                "return":"""name: "fraud_training"
                                    default_variant: "quickstart"
                                    variants: "quickstart"
                                    """

            },
            "variant":{
                "output":"""NAME:                          fraud_training
                                      VARIANT:                       quickstart
                                      OWNER:                         featureformer
                                      PROVIDER:                      postgres-quickstart
                                      STATUS:                        CREATED
                                      -----------------------------------------------
                                      LABEL:
                                      NAME                           VARIANT
                                      fraudulent                     quickstart
                                      -----------------------------------------------
                                      FEATURES:
                                      NAME                           VARIANT
                                      avg_transactions               quickstart
                                      -----------------------------------------------\n
                                """

            }

        }
    },
    "source": {
        "unit_test": {
            "object":Source(
        name="test_name",
        variant="test_variant",
        owner="test_owner",
        description="test_description",
        definition=PrimaryData(location=SQLTable(name="test_table")),
        provider="test_provider",
        status=ResourceStatus.CREATED,
        trainingsets=[("test_training_set","test_variant")],
        labels=[("test_label","test_variant")],
        features=[("test_feature","test_variant")]
    ),
        "output":"""NAME:                          test_name
VARIANT:                       test_variant
OWNER:                         test_owner
DESCRIPTION:                   test_description
PROVIDER:                      test_provider
STATUS:                        CREATED
-----------------------------------------------
DEFINITION:
PRIMARY DATA LOCATION          test_table
-----------------------------------------------
FEATURES:
NAME                           VARIANT
test_feature                   test_variant
-----------------------------------------------
LABELS:
NAME                           VARIANT
test_label                     test_variant
-----------------------------------------------
TRAINING SETS:
NAME                           VARIANT
test_training_set              test_variant
-----------------------------------------------
"""
        },
        "e2e_test": {
            "object":{
                "output":"""NAME:                          transactions
                          STATUS:                        CREATED
                          -----------------------------------------------
                          VARIANTS:
                          kaggle                         default
                          -----------------------------------------------\n
                          """,


            },
            "variant":{
                "output":"""NAME:                          transactions
                                VARIANT:                       kaggle
                                OWNER:                         featureformer
                                DESCRIPTION:                   Fraud Dataset From Kaggle
                                PROVIDER:                      postgres-quickstart
                                STATUS:                        CREATED
                                -----------------------------------------------
                                DEFINITION:
                                TRANSFORMATION

                                -----------------------------------------------
                                SOURCES
                                NAME                           VARIANT
                                -----------------------------------------------
                                PRIMARY DATA
                                Transactions
                                FEATURES:
                                NAME                           VARIANT
                                -----------------------------------------------
                                LABELS:
                                NAME                           VARIANT
                                fraudulent                     quickstart
                                -----------------------------------------------
                                TRAINING SETS:
                                NAME                           VARIANT
                                fraud_training                 quickstart
                                -----------------------------------------------\n
                                """
            }
            
        }
    },
    "entity": {
        "unit_test": {
            "object":Entity(
        name="test_entity",
        description="",
        features=[("test_training_set","test_variant")],
        labels=[("test_training_set","test_variant")],
        trainingsets=[("test_training_set","test_variant")]
    ),
            "output":"""ENTITY NAME:                   test_entity
-----------------------------------------------
NAME                           VARIANT                        TYPE
test_training_set              test_variant                   feature
test_training_set              test_variant                   label
test_training_set              test_variant                   training set
-----------------------------------------------
"""

        },
        "e2e_test": {
            "output":"""ENTITY NAME:                   user
                          STATUS:                        CREATED
                          -----------------------------------------------

                          NAME                           VARIANT                        TYPE
                          avg_transactions               quickstart                     feature
                          fraudulent                     quickstart                     label
                          fraud_training                 quickstart                     training set
                          -----------------------------------------------\n
                          """,

        }
    },
    "provider": {
        "unit_test": {
            "object":Provider(
        name="test_name",
        description="test_description",
        function="",
        config=LocalConfig(),
        team="featureform team",
        sources=[("test_source","test_variant")],
        trainingsets=[("test_training_set","test_variant")],
        labels=[("test_label","test_variant")],
        features=[("test_feature","test_variant")]
    ),
        "output":"""NAME:                          test_name
DESCRIPTION:                   test_description
TYPE:                          LOCAL_ONLINE
SOFTWARE:                      localmode
TEAM:                          featureform team
-----------------------------------------------
SOURCES:
NAME                           VARIANT
test_source                    test_variant
-----------------------------------------------
FEATURES:
NAME                           VARIANT
test_feature                   test_variant
-----------------------------------------------
LABELS:
NAME                           VARIANT
test_label                     test_variant
-----------------------------------------------
TRAINING SETS:
NAME                           VARIANT
test_training_set              test_variant
-----------------------------------------------
"""

        },
        "e2e_test": {
            "output":"""NAME:                          redis-quickstart
                              DESCRIPTION:                   A Redis deployment we created for the Featureform quickstart
                              TYPE:                          REDIS_ONLINE
                              SOFTWARE:                      redis
                              STATUS:                        CREATED
                              -----------------------------------------------
                              SOURCES:
                              NAME                           VARIANT
                              -----------------------------------------------
                              FEATURES:
                              NAME                           VARIANT
                              avg_transactions               quickstart
                              -----------------------------------------------
                              LABELS:
                              NAME                           VARIANT
                              -----------------------------------------------
                              TRAINING SETS:
                              NAME                           VARIANT
                              -----------------------------------------------\n
                              """

        }
    },
    "model": {
        "unit_test": {
            "object":Model(
        name="test_name",
        description="test_model",
        features=[("test_training_set","test_variant")],
        labels=[("test_training_set","test_variant")],
        trainingsets=[("test_training_set","test_variant")]
    ),
        "output":"""MODEL NAME:                    test_name
MODEL DESC:                    test_model
-----------------------------------------------
NAME                           VARIANT                        TYPE
test_training_set              test_variant                   feature
test_training_set              test_variant                   label
test_training_set              test_variant                   training set
-----------------------------------------------
"""

        },
        "e2e_test": {

        }
    },
    "user": {
        "unit_test": {
            "object":User(
        name="test_name",
        features=[("test_training_set","test_variant")],
        labels=[("test_training_set","test_variant")],
        trainingsets=[("test_training_set","test_variant")]
    ),
            "output":"""USER NAME:                     test_name
-----------------------------------------------
NAME                           VARIANT                        TYPE
test_training_set              test_variant                   feature
test_training_set              test_variant                   label
test_training_set              test_variant                   training set
-----------------------------------------------
"""

        },
        "e2e_test": {
            "output":"""USER NAME:                     featureformer
                          -----------------------------------------------

                          NAME                           VARIANT                        TYPE
                          avg_transactions               quickstart                     feature
                          fraudulent                     quickstart                     label
                          fraud_training                 quickstart                     training set
                          transactions                   kaggle                         source
                          average_user_transaction       quickstart                     source
                          -----------------------------------------------\n
                          """

        }
    },
    
}
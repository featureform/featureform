from .resources import Feature, Label, Source, TrainingSet, Entity, Model, Provider, User, ResourceStatus

print_outputs = {
    "feature": {
        "unit_test": {
            "resource":Feature(
        name="test_name",
        variant="test_variant",
        owner="test_owner",
        value_type="float32",
        description="test_description",
        entity="test_entity",
        provider="test_provider",
        status=ResourceStatus.NO_STATUS,
        source=("test_source","test_variant")
        trainingsets=[("test_training_set","test_variant")]
    ),
            "output":"""
        NAME:                          test_name
        VARIANT:                       test_variant
        TYPE:                          float32
        ENTITY:                        user
        OWNER:                         test_owner
        PROVIDER:                      test_provider
        DESCRIPTION:                   test_description
        STATUS:                        NO_STATUS
        -----------------------------------------------
        SOURCE:
        NAME                           VARIANT
        test_source                   test_variant
        -----------------------------------------------
        TRAINING SETS:
        NAME                           VARIANT
        test_training_set                 test_variant
        -----------------------------------------------
    """

        },
        "e2e_test": {
            "resource":{
                "output":"""NAME:                          avg_transactions
                              STATUS:                        NO_STATUS
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
                                STATUS:                        NO_STATUS
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
            "resource":Label(
        name="test_name",
        variant="test_variant",
        owner="test_owner",
        description="test_description",
        entity="test_entity",
        provider="test_provider",
        status=ResourceStatus.NO_STATUS,
        source=("test_source","test_variant")
        trainingsets=[("test_training_set","test_variant")]
    ),
        "output":"""
        NAME:                          test_name
        VARIANT:                       test_variant
        TYPE:                          float32
        ENTITY:                        user
        OWNER:                         test_owner
        PROVIDER:                      test_provider
        DESCRIPTION:                   test_description
        STATUS:                        NO_STATUS
        -----------------------------------------------
        SOURCE:
        NAME                           VARIANT
        test_source                   test_variant
        -----------------------------------------------
        TRAINING SETS:
        NAME                           VARIANT
        test_training_set                 test_variant
        -----------------------------------------------
    """

        },
        "e2e_test": {
            "resource":{
                "output":"""NAME:                          fraudulent
                            STATUS:                        NO_STATUS
                            -----------------------------------------------
                            VARIANTS:
                            quickstart                     default
                            -----------------------------------------------\n
                          """
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
                                  STATUS:                        NO_STATUS
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
            "resource":TrainingSet(
        name="test_name",
        variant="test_variant",
        owner="test_owner",
        description="test_description",
        label=("test_label","test_variant"),
        provider="test_provider",
        status=ResourceStatus.NO_STATUS,
        features=[("test_feature","test_variant")]
    ),
        "output":"""
        NAME:                          test_name
        VARIANT:                       test_variant
        OWNER:                         test_owner
        PROVIDER:                      test_provider
        DESCRIPTION:                   test_description
        STATUS:                        NO_STATUS
        -----------------------------------------------
        LABEL:
        NAME                           VARIANT
        test_label                   test_variant
        -----------------------------------------------
        FEATURES:
        NAME                           VARIANT
        test_feature                 test_variant
        -----------------------------------------------
    """
        },
        "e2e_test": {
            "resource"{
                "output":"""NAME:                          fraud_training
                                  STATUS:                        NO_STATUS
                                  -----------------------------------------------
                                  VARIANTS:
                                  quickstart                     default
                                  -----------------------------------------------\n
                                  """
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
                                      STATUS:                        NO_STATUS
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
            "resource":Source(
        name="test_name",
        variant="test_variant",
        owner="test_owner",
        description="test_description",
        provider="test_provider",
        status=ResourceStatus.NO_STATUS,
        sources=[("test_source","test_variant")]
        trainingsets=[("test_training_set","test_variant")],
        labels=[("test_label","test_variant")]
    ),
        "output":"""
        NAME:                          test_name
        VARIANT:                       test_variant
        OWNER:                         test_owner
        DESCRIPTION:                   test_description
        PROVIDER:                      test_provider
        STATUS:                        NO_STATUS
        -----------------------------------------------
        DEFINITION:
        TRANSFORMATION  

        -----------------------------------------------
        SOURCES
        NAME                           VARIANT
        test_source                    test_variant
        -----------------------------------------------
        PRIMARY DATA
        Transactions
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
            "resource":{
                "output":"""NAME:                          transactions
                          STATUS:                        NO_STATUS
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
                                STATUS:                        NO_STATUS
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
            "resource":Entity(
        name="test_entity",
        features=[("test_training_set","test_variant")],
        labels=[("test_training_set","test_variant")],
        trainingsets=[("test_training_set","test_variant")]
    ),
            "output":"""
        ENTITY NAME:                   test_entity
        -----------------------------------------------

        NAME                           VARIANT                        TYPE
        test_feature                   test_variant                     feature
        test_label                     test_variant                     label
        test_training_set              test_variant                    training set
        -----------------------------------------------
    """

        },
        "e2e_test": {
            "output":"""ENTITY NAME:                   user
                          STATUS:                        NO_STATUS
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
            "resource":Provider(
        name="test_name",
        variant="test_variant",
        owner="test_owner",
        description="test_description",
        provider="test_provider",
        status=ResourceStatus.NO_STATUS,
        sources=[("test_source","test_variant")]
        trainingsets=[("test_training_set","test_variant")],
        labels=[("test_label","test_variant")],
        features=[("test_feature","test_variant")]
    ),
        "output":"""
        NAME:                          test_provider
        DESCRIPTION:                   test_description
        TYPE:                          POSTGRES_OFFLINE
        SOFTWARE:                      postgres
        STATUS:                        NO_STATUS
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
                              STATUS:                        NO_STATUS
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
            "resource":Model(
        name="test_name",
        features=[("test_training_set","test_variant")],
        labels=[("test_training_set","test_variant")],
        trainingsets=[("test_training_set","test_variant")]
    ),
        "output":"""
        USER NAME:                     featureformer
        -----------------------------------------------

        NAME                           VARIANT                        TYPE
        test_feature                   test_variant                     feature
        test_label                     test_variant                     label
        test_training_set              test_variant                    training set
        -----------------------------------------------
    """

        },
        "e2e_test": {

        }
    },
    "user": {
        "unit_test": {
            "resource":User(
        name="test_name",
        features=[("test_training_set","test_variant")],
        labels=[("test_training_set","test_variant")],
        trainingsets=[("test_training_set","test_variant")]
    ),
            "output":"""
        USER NAME:                     featureformer
        -----------------------------------------------

        NAME                           VARIANT                        TYPE
        test_feature                   test_variant                     feature
        test_label                     test_variant                     label
        test_training_set              test_variant                    training set
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
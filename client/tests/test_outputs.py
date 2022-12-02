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
                "return":r"""name: "avg_transactions"
                                  variant: "quickstart"
                                  source {
                                    name: "average_user_transaction"
                                    variant: "quickstart"
                                  }
                                  type: "float32"
                                  entity: "user"
                                  created {
                                    seconds: [0-9]+
                                    nanos: [0-9]+
                                  }
                                  owner: "featureformer"
                                  provider: "redis-quickstart"
                                  trainingsets {
                                    name: "fraud_training"
                                    variant: "quickstart"
                                  }
                                  columns {
                                    entity: "user_id"
                                    value: "avg_transaction_amt"
                                  }
                                  """
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
                "return":r"""name: "fraudulent"
                                  variant: "quickstart"
                                  type: "bool"
                                  source {
                                    name: "transactions"
                                    variant: "kaggle"
                                  }
                                  entity: "user"
                                  created {
                                    seconds: [0-9]+
                                    nanos: [0-9]+
                                  }
                                  owner: "featureformer"
                                  provider: "postgres-quickstart"
                                  trainingsets {
                                    name: "fraud_training"
                                    variant: "quickstart"
                                  }
                                  columns {
                                    entity: "customerid"
                                    value: "isfraud"
                                  }
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
                "return":r"""name: "fraud_training"
                                        variant: "quickstart"
                                        owner: "featureformer"
                                        created {
                                          seconds: [0-9]+
                                          nanos: [0-9]+
                                        }
                                        provider: "postgres-quickstart"
                                        features {
                                          name: "avg_transactions"
                                          variant: "quickstart"
                                        }
                                        label {
                                          name: "fraudulent"
                                          variant: "quickstart"
                                        }
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
            "return":r"""name: "transactions"
                            default_variant: "kaggle"
                            variants: "kaggle"
                            """


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
                "return":r"""name: "transactions"
                                  variant: "kaggle"
                                  owner: "featureformer"
                                  description: "Fraud Dataset From Kaggle"
                                  provider: "postgres-quickstart"
                                  created {
                                    seconds: [0-9]+
                                    nanos: [0-9]+
                                  }
                                  trainingsets {
                                    name: "fraud_training"
                                    variant: "quickstart"
                                  }
                                  labels {
                                    name: "fraudulent"
                                    variant: "quickstart"
                                  }
                                  primaryData {
                                    table {
                                      name: "Transactions"
                                    }
                                  }
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
            "return":r"""name: "user"
                            features {
                              name: "avg_transactions"
                              variant: "quickstart"
                            }
                            labels {
                              name: "fraudulent"
                              variant: "quickstart"
                            }
                            trainingsets {
                              name: "fraud_training"
                              variant: "quickstart"
                            }
                            """

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
            "return":r"""name: "redis-quickstart"
                                description: "A Redis deployment we created for the Featureform quickstart"
                                type: "REDIS_ONLINE"
                                software: "redis"
                                serialized_config: "{\\"Addr\\":\\"quickstart-redis:6379\\", \\"Password\\": \\"\\", \\"DB\\": 0}"
                                features {
                                  name: "avg_transactions"
                                  variant: "quickstart"
                                }
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
            "return":r"""name: "featureformer"
                            features {
                              name: "avg_transactions"
                              variant: "quickstart"
                            }
                            labels {
                              name: "fraudulent"
                              variant: "quickstart"
                            }
                            trainingsets {
                              name: "fraud_training"
                              variant: "quickstart"
                            }
                            sources {
                              name: "transactions"
                              variant: "kaggle"
                            }
                            sources {
                              name: "average_user_transaction"
                              variant: "quickstart"
                            }\n"""

        }
    },
    
}
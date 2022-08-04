from .register import Client
from io import StringIO
from unittest.mock import patch
import re
import os

rc = Client("localhost:8000", False, False, './tls.crt')

def check_print_return(expected_print, expected_return, resource_type):
  rc_get_functions = {
    "provider": [rc.get_provider, "redis-quickstart"],
    "entity": [rc.get_entity, "user"],
    "feature": [rc.get_feature, "avg_transactions"],
    "label": [rc.get_label, "fraudulent"],
    "user": [rc.get_user, "featureformer"],
    "source": [rc.get_source, "transactions"],
    "training_set": [rc.get_training_set, "fraud_training"]
  }

  rc_get_functions_var = {
    "source_var": [rc.get_source, "transactions", "kaggle"],
    "feature_var": [rc.get_feature, "avg_transactions", "quickstart"],
    "label_var": [rc.get_label, "fraudulent", "quickstart"],
    "training_set_var": [rc.get_training_set, "fraud_training", "quickstart"]
  }

  with patch('sys.stdout', new = StringIO()) as fake_out:
    if resource_type in rc_get_functions:
      value = rc_get_functions[resource_type][0](rc_get_functions[resource_type][1])
    if resource_type in rc_get_functions_var:
      value = rc_get_functions_var[resource_type][0](rc_get_functions_var[resource_type][1], rc_get_functions_var[resource_type][2])
    assert (fake_out.getvalue().replace(" ","") == expected_print.replace(" ", ""))
  assert re.match(expected_return.replace(" ", ""), str(value).replace(" ", ""))

def test_get_provider():
  expected_print_provider = """NAME:                          redis-quickstart
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
  expected_return_provider = r"""name: "redis-quickstart"
                                description: "A Redis deployment we created for the Featureform quickstart"
                                type: "REDIS_ONLINE"
                                software: "redis"
                                serialized_config: "{\\"Addr\\":\\"quickstart-redis:6379\\", \\"Password\\": \\"\\", \\"DB\\": 0}"
                                features {
                                  name: "avg_transactions"
                                  variant: "quickstart"
                                }
                                """
  check_print_return(expected_print_provider, expected_return_provider, "provider")
  

def test_get_entity():
  expected_print_entity = """ENTITY NAME:                   user
                          STATUS:                        NO_STATUS
                          -----------------------------------------------

                          NAME                           VARIANT                        TYPE
                          avg_transactions               quickstart                     feature
                          fraudulent                     quickstart                     label
                          fraud_training                 quickstart                     training set
                          -----------------------------------------------\n
                          """
  expected_return_entity = r"""name: "user"
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
  check_print_return(expected_print_entity, expected_return_entity, "entity")

def test_get_user():
  expected_print_user = """USER NAME:                     featureformer
                          -----------------------------------------------

                          NAME                           VARIANT                        TYPE
                          avg_transactions               quickstart                     feature
                          fraudulent                     quickstart                     label
                          fraud_training                 quickstart                     training set
                          transactions                   kaggle                         source
                          average_user_transaction       quickstart                     source
                          -----------------------------------------------\n
                          """
  expected_return_user = r"""name: "featureformer"
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
  check_print_return(expected_print_user, expected_return_user, "user")

def test_get_source():
  expected_print_source = """NAME:                          transactions
                          STATUS:                        NO_STATUS
                          -----------------------------------------------
                          VARIANTS:
                          kaggle                         default
                          -----------------------------------------------\n
                          """
  expected_return_source = r"""name: "transactions"
                            default_variant: "kaggle"
                            variants: "kaggle"
                            """
  check_print_return(expected_print_source, expected_return_source, "source")

def test_get_source_var():
  expected_print_source_var = """NAME:                          transactions
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
  expected_return_source_var = r"""name: "transactions"
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
  check_print_return(expected_print_source_var, expected_return_source_var, "source_var")

def test_get_feature():
  expected_print_feature = """NAME:                          avg_transactions
                              STATUS:                        NO_STATUS
                              -----------------------------------------------
                              VARIANTS:
                              quickstart                     default
                              -----------------------------------------------\n
                          """
  expected_return_feature = """name: "avg_transactions"
                              default_variant: "quickstart"
                              variants: "quickstart"
                              """
  check_print_return(expected_print_feature, expected_return_feature, "feature")

def test_get_feature_var():
  expected_print_feature_var = """NAME:                          avg_transactions
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
                                """
  expected_return_feature_var = r"""name: "avg_transactions"
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
  check_print_return(expected_print_feature_var, expected_return_feature_var, "feature_var")

def test_get_label():
  expected_print_label = """NAME:                          fraudulent
                            STATUS:                        NO_STATUS
                            -----------------------------------------------
                            VARIANTS:
                            quickstart                     default
                            -----------------------------------------------\n
                          """
  expected_return_label = """name: "fraudulent"
                              default_variant: "quickstart"
                              variants: "quickstart"
                              """
  check_print_return(expected_print_label, expected_return_label, "label")

def test_get_label_var():
  expected_print_label_var = """NAME:                          fraudulent
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
  expected_return_label_var = r"""name: "fraudulent"
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
  check_print_return(expected_print_label_var, expected_return_label_var, "label_var")

def test_get_training_set():
  expected_print_training_set = """NAME:                          fraud_training
                                  STATUS:                        NO_STATUS
                                  -----------------------------------------------
                                  VARIANTS:
                                  quickstart                     default
                                  -----------------------------------------------\n
                                  """
  expected_return_training_set = """name: "fraud_training"
                                    default_variant: "quickstart"
                                    variants: "quickstart"
                                    """
  check_print_return(expected_print_training_set, expected_return_training_set, "training_set")

def test_get_training_set_var():
  expected_print_training_set_var = """NAME:                          fraud_training
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
  expected_return_training_set_var = r"""name: "fraud_training"
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
  check_print_return(expected_print_training_set_var, expected_return_training_set_var, "training_set_var")

from .register import Client
from io import StringIO
from unittest.mock import patch
import os

rc = Client("localhost:8000", False, False, './tls.crt')

def check_print_return(expected_print, expected_return, resource_type):
  rc_list_functions = {
    "provider": rc.list_providers,
    "entity": rc.list_entities,
    "feature": rc.list_features,
    "label": rc.list_labels,
    "user": rc.list_users,
    "source": rc.list_sources,
    "training_set": rc.list_training_sets
  }

  with patch('sys.stdout', new = StringIO()) as fake_out:
    if resource_type in rc_list_functions:
      value = rc_list_functions[resource_type]()
    else:
        raise ValueError("resource type not in Resource Client list functions")
    assert (fake_out.getvalue().replace(" ","") == expected_print.replace(" ", ""))
  assert (str(value).replace(" ", "") ==  expected_return.replace(" ", ""))

def test_list_provider():
  expected_print_provider = """NAME                           STATUS                         DESCRIPTION
                            postgres-quickstart            NO_STATUS                      A Postgres deployment we created for the Featureform quickst
                            redis-quickstart               NO_STATUS                      A Redis deployment we created for the Featureform quickstart
                            """
  expected_return_provider = """[name: "postgres-quickstart"
                                description: "A Postgres deployment we created for the Featureform quickstart"
                                type: "POSTGRES_OFFLINE"
                                software: "postgres"
                                serialized_config: "{\\"Host\\": \\"quickstart-postgres\\", \\"Port\\": \\"5432\\", \\"Username\\": \\"postgres\\", \\"Password\\": \\"password\\", \\"Database\\": \\"postgres\\"}"
                                sources {
                                name: "transactions"
                                variant: "kaggle"
                                }
                                sources {
                                name: "average_user_transaction"
                                variant: "quickstart"
                                }
                                trainingsets {
                                name: "fraud_training"
                                variant: "quickstart"
                                }
                                labels {
                                name: "fraudulent"
                                variant: "quickstart"
                                }
                                , name: "redis-quickstart"
                                description: "A Redis deployment we created for the Featureform quickstart"
                                type: "REDIS_ONLINE"
                                software: "redis"
                                serialized_config: "{\\"Addr\\": \\"quickstart-redis:6379\\", \\"Password\\": \\"\\", \\"DB\\": 0}"
                                features {
                                name: "avg_transactions"
                                variant: "quickstart"
                                }
                                ]"""
  check_print_return(expected_print_provider, expected_return_provider, "provider")
  

def test_list_entity():
  expected_print_entity = """NAME                           STATUS
                            user                           NO_STATUS
                          """
  expected_return_entity = """[name: "user"
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
                            ]"""
  check_print_return(expected_print_entity, expected_return_entity, "entity")

def test_list_user():
  expected_print_user = """NAME                           STATUS
                        featureformer                  NO_STATUS
                        """
  expected_return_user = """[name: "featureformer"
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
                            }
                            ]"""
  check_print_return(expected_print_user, expected_return_user, "user")

def test_list_source():
  expected_print_source = """NAME                           VARIANT                        STATUS                         DESCRIPTION
                            average_user_transaction       quickstart (default)           NO_STATUS                      the average transaction amount for a user
                            transactions                   kaggle (default)               NO_STATUS                      Fraud Dataset From Kaggle
                            """
  expected_return_source = """[name: "average_user_transaction"
                            default_variant: "quickstart"
                            variants: "quickstart"
                            , name: "transactions"
                            default_variant: "kaggle"
                            variants: "kaggle"
                            ]"""
  check_print_return(expected_print_source, expected_return_source, "source")

def test_list_feature():
  expected_print_feature = """NAME                           VARIANT                        STATUS
                            avg_transactions               quickstart (default)           NO_STATUS
                            """
  expected_return_feature = """[name: "avg_transactions"
                                default_variant: "quickstart"
                                variants: "quickstart"
                                ]"""
  check_print_return(expected_print_feature, expected_return_feature, "feature")

def test_list_label():
  expected_print_label = """NAME                           VARIANT                        STATUS
                            fraudulent                     quickstart (default)           NO_STATUS
                            """
  expected_return_label = """[name: "fraudulent"
                            default_variant: "quickstart"
                            variants: "quickstart"
                            ]"""
  check_print_return(expected_print_label, expected_return_label, "label")

def test_list_training_set():
  expected_print_training_set = """NAME                           VARIANT                        STATUS                         DESCRIPTION
                                fraud_training                 quickstart (default)           NO_STATUS
                                """
  expected_return_training_set = """[name: "fraud_training"
                                    default_variant: "quickstart"
                                    variants: "quickstart"
                                    ]"""
  check_print_return(expected_print_training_set, expected_return_training_set, "training_set")
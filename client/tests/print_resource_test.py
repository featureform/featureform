import pytest
import featureform as ff
from featureform.resources import TrainingSet, Feature, ResourceColumnMapping
from featureform.serving import ServingClient

from datetime import timedelta

def test_feature_print():
    feature = Feature(
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
    )

    expected_feature_print = """
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

    assert expected_feature_print == f"{feature}"

    
def test_training_set_print():
    training_set = TrainingSet(
        name="test_name",
        variant="test_variant",
        owner="test_owner",
        description="test_description",
        label=("test_label","test_variant"),
        provider="test_provider",
        status=ResourceStatus.NO_STATUS,
        features=[("test_feature","test_variant")]
    )

    expected_training_set_print = """
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

    assert expected_training_set_print == f"{training_set}"
    
def test_source_print():
    source = Source(
        name="test_name",
        variant="test_variant",
        owner="test_owner",
        description="test_description",
        provider="test_provider",
        status=ResourceStatus.NO_STATUS,
        sources=[("test_source","test_variant")]
        trainingsets=[("test_training_set","test_variant")],
        labels=[("test_label","test_variant")]
    )

    expected_source_print = """
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

    assert expected_source_print == f"{source}"
    
def test_label_print():
    label = Label(
        name="test_name",
        variant="test_variant",
        owner="test_owner",
        description="test_description",
        entity="test_entity",
        provider="test_provider",
        status=ResourceStatus.NO_STATUS,
        source=("test_source","test_variant")
        trainingsets=[("test_training_set","test_variant")]
    )

    expected_label_print = """
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

    assert expected_label_print == f"{label}"
    
def test_entity_print():
    entity = Entity(
        name="test_entity",
        features=[("test_training_set","test_variant")],
        labels=[("test_training_set","test_variant")],
        trainingsets=[("test_training_set","test_variant")]
    )

    expected_entity_print = """
        ENTITY NAME:                   test_entity
        -----------------------------------------------

        NAME                           VARIANT                        TYPE
        test_feature                   test_variant                     feature
        test_label                     test_variant                     label
        test_training_set              test_variant                    training set
        -----------------------------------------------
    """

    assert expected_entity_print == f"{entity}"
    
def test_user_print():
    user = User(
        name="test_name",
        features=[("test_training_set","test_variant")],
        labels=[("test_training_set","test_variant")],
        trainingsets=[("test_training_set","test_variant")]
    )

    expected_user_print = """
        USER NAME:                     featureformer
        -----------------------------------------------

        NAME                           VARIANT                        TYPE
        test_feature                   test_variant                     feature
        test_label                     test_variant                     label
        test_training_set              test_variant                    training set
        -----------------------------------------------
    """

    assert expected_user_print == f"{user}"
    
def test_model_print():
    model = Model(
        name="test_name",
        features=[("test_training_set","test_variant")],
        labels=[("test_training_set","test_variant")],
        trainingsets=[("test_training_set","test_variant")]
    )

    expected_model_print = """
        USER NAME:                     featureformer
        -----------------------------------------------

        NAME                           VARIANT                        TYPE
        test_feature                   test_variant                     feature
        test_label                     test_variant                     label
        test_training_set              test_variant                    training set
        -----------------------------------------------
    """

    assert expected_model_print == f"{model}"
    
def test_provider_print():
    provider = Provider(
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
    )

    expected_provider_print = """
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

    assert expected_provider_print == f"{provider}"
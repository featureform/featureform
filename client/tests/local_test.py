import featureform as ff
from featureform import local
import pandas as pd
import pytest
import shutil
import os
import stat

class TestPetalGuide:
    def test_register_local(self):

        iris = local.register_file(
            name="Iris dataset",
            description="Iris dataset from Kaggle",
            path="iris.csv"
        )

        test_entity = ff.register_entity("id")

        @local.df_transformation(variant="v1", inputs=[("Iris dataset", "Kaggle")])
        def base_transformation1(df):
            """the number of transactions for each user"""
            df.drop(columns=["SepalLengthCm", "SepalWidthCm"], inplace=True)
            return df

        @local.df_transformation(variant="v1", inputs=[("Iris dataset", "Kaggle")])
        def base_transformation2(df):
            """the number of transactions for each user"""
            df.drop(columns=["PetalLengthCm", "PetalWidthCm", "Species"], inplace=True)
            return df

        @local.df_transformation(variant="v1", inputs=[("base_transformation1", "v1"), ("base_transformation2", "v1")])
        def join_transformation(df1, df2):
            """the number of transactions for each user"""
            return df2.assign(PetalLengthCm=df1['PetalLengthCm'], PetalWidthCm=df1['PetalWidthCm'], Species=df1['Species'])


        @local.df_transformation(variant="v1", inputs=[("Iris dataset", "Kaggle"), ("Iris dataset", "Kaggle")])
        def new_transformation(df, df2):
            """the number of transactions for each user"""
            df.drop(columns="Species", inplace=True)
            return df.transform(lambda x: x + 1)

        @local.df_transformation(variant="v1", inputs=[("Iris dataset", "Kaggle")])
        def transform1(df):
            """one transform"""
            df = pd.DataFrame({'id':[1, 2, 3, 1], 'value': ["one", "two", "three", "four"], 'ts': [.1, .2, .3, .4]})
            return df

        @local.df_transformation(variant="v1", inputs=[("Iris dataset", "Kaggle")])
        def transform2(df):
            """one transform"""
            df = pd.DataFrame({'id':[1, 2, 3], 'value': ["three", "four", "five"], 'ts': [.3, .2, .1]})
            return df

        @local.df_transformation(variant="v1", inputs=[("Iris dataset", "Kaggle")])
        def transform3(df):
            """one transform"""
            df = pd.DataFrame({'id':[1, 2, 3], 'value': [True, False, True], 'ts': [.2, .2, .2]})
            return df

        transform1.register_resources(
            entity=test_entity,
            entity_column="id",
            inference_store=local,
            features=[
                {"name": "feat1", "variant": "v1", "column": "value", "type": "string"},
            ],
            timestamp_column='ts'
        )

        transform1.register_resources(
            entity=test_entity,
            entity_column="id",
            inference_store=local,
            features=[
                {"name": "feat1", "variant": "v2", "column": "value", "type": "string"},
            ],
        )

        transform2.register_resources(
            entity=test_entity,
            entity_column="id",
            inference_store=local,
            features=[
                {"name": "feat2", "variant": "v1", "column": "value", "type": "string"},
            ],
            timestamp_column='ts'
        )

        transform2.register_resources(
            entity=test_entity,
            entity_column="id",
            inference_store=local,
            features=[
                {"name": "feat2", "variant": "v2", "column": "value", "type": "string"},
            ],
        )

        transform3.register_resources(
            entity=test_entity,
            entity_column="id",
            inference_store=local,
            labels=[
                {"name": "label1", "variant": "v1", "column": "value", "type": "bool"},
            ],
            timestamp_column='ts'
        )

        transform3.register_resources(
            entity=test_entity,
            entity_column="id",
            inference_store=local,
            labels=[
                {"name": "label1", "variant": "v2", "column": "value", "type": "bool"},
            ],
        )

        user_entity = ff.register_entity("flower")

        new_transformation.register_resources(
            entity=user_entity,
            entity_column="Id",
            inference_store=local,
            features=[
                {"name": "SepalLength", "variant": "transformation_test", "column": "SepalLengthCm", "type": "float"},
            ],

        )

        join_transformation.register_resources(
            entity=user_entity,
            entity_column="Id",
            inference_store=local,
            features=[
                {"name": "SepalLength", "variant": "join", "column": "SepalLengthCm", "type": "float"},
                {"name": "SepalWidth", "variant": "join", "column": "SepalWidthCm", "type": "float"},
                {"name": "PetalLength", "variant": "join", "column": "PetalLengthCm", "type": "float"},
                {"name": "PetalWidth", "variant": "join", "column": "PetalWidthCm", "type": "float"},
            ],
            labels=[
                {"name": "SpeciesType", "variant": "join", "column": "Species", "type": "Label"},
            ],

        )
        iris.register_resources(
            entity=user_entity,
            entity_column="Id",
            inference_store=local,
            features=[
                {"name": "SepalLength", "variant": "centimeters", "column": "SepalLengthCm", "type": "float"},
                {"name": "SepalWidth", "variant": "centimeters", "column": "SepalWidthCm", "type": "float"},
                {"name": "PetalLength", "variant": "centimeters", "column": "PetalLengthCm", "type": "float"},
                {"name": "PetalWidth", "variant": "centimeters", "column": "PetalWidthCm", "type": "float"},
            ],

            labels=[
                {"name": "SpeciesType", "variant": "String", "column": "Species", "type": "Label"},
            ],
        )

        ff.register_training_set(
            "test_training", "v1",
            label=("label1", "v1"),
            features=[("feat1", "v1"), ("feat2", "v1")],
        )

        ff.register_training_set(
            "test_training", "v2",
            label=("label1", "v2"),
            features=[("feat1", "v2"), ("feat2", "v2")],
        )

        ff.register_training_set(
            "iris_training", "quickstart",
            label=("SpeciesType", "String"),
            features=[("SepalLength", "centimeters"), ("SepalWidth", "centimeters"), ("PetalLength", "centimeters"),
                      ("PetalWidth", "centimeters"), ("SepalLength", "transformation_test")],
        )

        ff.register_training_set(
            "join", "v1",
            label=("SpeciesType", "join"),
            features=[("SepalLength", "join"), ("SepalWidth", "join"), ("PetalLength", "join"),
                      ("PetalWidth", "join")],
        )

        client = ff.ResourceClient(local=True)
        client.apply()

    def test_invalid_label(self):

        ff.register_training_set(
            "join", "v2",
            label=("SpeciesTypo", "join"),
            features=[("SepalLength", "join"), ("SepalWidth", "join"), ("PetalLength", "join"),
                      ("PetalWidth", "join")],
        )
        with pytest.raises(ValueError) as err:
            client = ff.ResourceClient(local=True)
            client.apply()
        assert "SpeciesTypo does not exist. Failed to register training set" in str(err.value)


    @pytest.fixture(autouse=True)
    def run_before_and_after_tests(tmpdir):
        """Fixture to execute asserts before and after a test is run"""
        # Remove any lingering Databases
        try:
            shutil.rmtree('.featureform', onerror=del_rw)
        except:
            print("File Already Removed")
        yield
        try:
            shutil.rmtree('.featureform', onerror=del_rw)
        except:
            print("File Already Removed")


def del_rw(action, name, exc):
    os.chmod(name, stat.S_IWRITE)
    os.remove(name)
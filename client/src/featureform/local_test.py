from multiprocessing.sharedctypes import Value
import featureform as ff
import pandas as pd
local = ff.register_local()

ff.register_user("featureformer").make_default_owner()

iris = local.register_file(
    name="Iris dataset",
    variant="Kaggle",
    description="Iris dataset from Kaggle",
    path="iris.csv"
)

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



# @local.df_transformation(variant="v1", inputs=[("iris_dataset", "v1"),
#                                                ("other_dataset", "v1")])
# def new_transformation(iris, other):
#     """the number of transactions for each user"""
#     return iris.join(other.setIndex('ID'))

test_entity = ff.register_entity("id")

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
    #
    # labels=[
    #     {"name": "SpeciesType", "variant": "String", "column": "Species", "type": "Label"},
    # ],

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



# Register a feature and a label
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

# Should error
ff.register_training_set(
"join", "v2",
label=("SpeciesTypo", "join"),
features=[("SepalLength", "join"), ("SepalWidth", "join"), ("PetalLength", "join"),
            ("PetalWidth", "join")],
)
try:
    client = ff.ResourceClient(local=True)
    client.apply()
except ValueError as e:
    print("Label does not exist")

# Should error
ff.register_training_set(
"join", "v3",
label=("SpeciesType", "join"),
features=[("SepalLength", "join"), ("SepalWidth", "join"), ("PetaLength", "join"),
            ("PetalWidth", "join")],
)
try:
    client = ff.ResourceClient(local=True)
    client.apply()
except ValueError:
    print("Feature not found")
    pass

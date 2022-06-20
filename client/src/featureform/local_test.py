import featureform as ff

local = ff.register_local()

local.register_file(
    name = "Iris dataset",
    variant = "Kaggle",
    description = "Iris dataset from Kaggle",
    path = "iris.csv" 
)

ff.register_user("featureformer").make_default_owner()

user_entity = ff.register_entity("flower")
# Register a feature
local.register_resources(
    entity=user_entity,
    entity_column="Id",
    inference_store=local,
    features=[
        {"name": "SepalLength", "variant": "centimeters", "column": "SepalLengthCm", "type": "float64"},
    ],
)
# Register a label
local.register_resources(
    entity=user_entity,
    entity_column="Id",
    labels=[
        {"name": "SpeciesType", "variant": "String", "column": "Species", "type": "string"},
    ],
)

ff.register_training_set(
    "iris_training", "quickstart",
    label=("SpeciesType", "String"),
    features=[("SepalLength", "centimeters")],
)
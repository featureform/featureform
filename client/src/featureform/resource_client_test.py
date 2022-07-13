import featureform as ff
from featureform import ResourceClient

rc = ResourceClient("localhost:8000")

redis = rc.get_provider("redis-quickstart")

print(redis)

postgres = rc.get_provider("postgres-quickstart")

print(postgres)

transactions = rc.get_source("transactions")

print(transactions)

transactions_variant = rc.get_source("transactions", "kaggle")

print(transactions_variant)

feature = rc.get_feature("avg_transactions")

print(feature)

feature_variant = rc.get_feature("avg_transactions", "quickstart")

print(feature_variant)

label = rc.get_label("fraudulent")

print(label)

label_variant = rc.get_label("fraudulent", "quickstart")

print(label_variant)

entity = rc.get_entity("user")

print(entity)

trainingset = rc.get_training_set("fraud_training")

print(trainingset)

trainingset_variant = rc.get_training_set("fraud_training", "quickstart")

print(trainingset_variant)

listProvider = rc.list_providers()

print(listProvider)

listSource = rc.list_sources()

print(listSource)

listFeature = rc.list_features()

print(listFeature)

listLabel = rc.list_labels()

print(listLabel)

listEntity = rc.list_entities()

print(listEntity)

listTrainingSet = rc.list_training_sets()

print(listTrainingSet)
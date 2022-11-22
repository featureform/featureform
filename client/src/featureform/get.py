from featureform.proto import metadata_pb2
import grpc
from featureform.proto import metadata_pb2_grpc as ff_grpc
from .format import *
import featureform as ff
from .resources import Model

def get_user_info(stub, name):
    searchName = metadata_pb2.Name(name=name)
    try:
        for user in stub.GetUsers(iter([searchName])):
            return user
    except grpc._channel._MultiThreadedRendezvous:
        print("User not found.")

def get_entity_info(stub, name):
    searchName = metadata_pb2.Name(name=name)
    try:
        for x in stub.GetEntities(iter([searchName])):
            return ff.Entity(
                name=x.name,
                description=x.description
            )
    except grpc._channel._MultiThreadedRendezvous:
        print("Entity not found.")

def get_resource_info(stub, resource_type, name):
    stub_get_functions = {
        "feature": stub.GetFeatures,
        "label": stub.GetLabels,
        "source": stub.GetSources,
        "trainingset": stub.GetTrainingSets,
        "training-set": stub.GetTrainingSets,
        "model": stub.GetModels
    }
    # Todo: change the parameters submitted to each one base on type
    searchName = metadata_pb2.Name(name=name)
    try:
        for x in stub_get_functions[resource_type](iter([searchName])):
            if resource_type == "feature":
                return Feature(
                    name=x.name
                )
            elif resource_type == "label":
                return Label(
                    name=x.name
                )
            elif resource_type == "source":
                return Source(
                    name=x.name
                )
            elif resource_type == "trainingset" or resource_type == "training-set":
                return TrainingSet(
                    name=x.name
                )
            elif resource_type == "model":
                return Model(
                    name=x.name
                )
    except grpc._channel._MultiThreadedRendezvous:
        print(f"{resource_type} not found.")

def get_feature_variant_info(stub, name, variant):
    searchNameVariant = metadata_pb2.NameVariant(name=name, variant=variant)
    try:
        for x in stub.GetFeatureVariants(iter([searchNameVariant])):                
            return ff.Feature(
                name=x.name,
                value_type=x.type,
                variant=x.variant,
                source=(x.source.name,x.source.variant),
                entity=x.entity,
                owner=x.owner,
                provider=x.provider,
                location=None,    
                status=x.status.Status._enum_type.values[x.status.status].name,
                description=x.description,
            )
    except grpc._channel._MultiThreadedRendezvous as e:
        print(f"Feature variant {name}:{variant} not found: {e}")
        raise

def get_label_variant_info(stub, name, variant):
    searchNameVariant = metadata_pb2.NameVariant(name=name, variant=variant)
    try:
        for x in stub.GetLabelVariants(iter([searchNameVariant])):
            return ff.Label(
                name=x.name,
                source=(x.source.name,x.source.variant),
                value_type=x.type,
                entity=x.entity,
                owner=x.owner,
                provider=x.provider,
                description=x.description,
                location=None,
                variant=x.variant,
                status=x.status.Status._enum_type.values[x.status.status].name
            )
    except grpc._channel._MultiThreadedRendezvous:
        print("Label variant not found.")

def get_source_variant_info(stub, name, variant):
    searchNameVariant = metadata_pb2.NameVariant(name=name, variant=variant)
    try:
        for x in stub.GetSourceVariants(iter([searchNameVariant])):
            definition = None
            is_transformation = None
            if x.primaryData.table.name:
                definition = ff.PrimaryData(location=x.primaryData.table.name)
                is_transformation = False
            elif x.transformation.SQLTransformation.query:
                definition = ff.SQLTransformation(query=x.transformation.SQLTransformation.query)
                is_transformation = True
            return ff.Source(
                name=x.name,
                definition=definition,
                description=x.description,
                variant=x.variant,
                provider=x.provider,
                owner=x.owner,
                status=x.status.Status._enum_type.values[x.status.status].name,
            )
    except grpc._channel._MultiThreadedRendezvous:
        print("Source variant not found.")

def get_training_set_variant_info(stub, name, variant):
    searchNameVariant = metadata_pb2.NameVariant(name=name, variant=variant)
    try:
        for x in stub.GetTrainingSetVariants(iter([searchNameVariant])):
            return ff.TrainingSet(
                name=x.name,
                variant=x.variant,
                label=(x.label.name,x.label.variant),
                status=x.status.Status._enum_type.values[x.status.status].name,
                description=x.description,
                owner=x.owner,
                schedule=x.schedule,
                features=[(f.name,f.variant) for f in x.features],
                feature_lags=None,
            )
    except grpc._channel._MultiThreadedRendezvous:
        print("Training set variant not found.")

def get_provider_info(stub, name):
    searchName = metadata_pb2.Name(name=name)
    try:
        for x in stub.GetProviders(iter([searchName])):
            return ff.Provider(
                name=x.name,
                description=x.description,
                team=x.team
            )
    except grpc._channel._MultiThreadedRendezvous:
        print("Provider not found.")
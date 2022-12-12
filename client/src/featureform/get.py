from featureform.proto import metadata_pb2
import grpc
from featureform.proto import metadata_pb2_grpc as ff_grpc
from featureform.resources import Feature, Label, TrainingSet, Source, Model, Entity, User, Provider, ResourceStatus, ResourceColumnMapping, PrimaryData, SQLTransformation, DFTransformation, SQLTable
from .format import *

def get_user_info(stub, name):
    searchName = metadata_pb2.Name(name=name)
    try:
        for user in stub.GetUsers(iter([searchName])):
            return User(
                name=user.name,
                features=[(f.name,f.variant) for f in user.features],
                labels=[(f.name,f.variant) for f in user.labels],
                trainingsets=[(f.name,f.variant) for f in user.trainingsets],
                sources=[(f.name,f.variant) for f in user.sources]
            )
    except grpc._channel._MultiThreadedRendezvous:
        print("User not found.")

def get_entity_info(stub, name):
    searchName = metadata_pb2.Name(name=name)
    try:
        for entity in stub.GetEntities(iter([searchName])):
            return Entity(
                name=x.name,
                description=x.description,
                features=[(f.name,f.variant) for f in entity.features],
                labels=[(f.name,f.variant) for f in entity.labels],
                trainingsets=[(f.name,f.variant) for f in entity.trainingsets],
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

    searchName = metadata_pb2.Name(name=name)
    try:
        for x in stub_get_functions[resource_type](iter([searchName])):
            format_rows([("NAME: ", x.name),
            ("STATUS: ", x.status.Status._enum_type.values[x.status.status].name)])
            format_pg("VARIANTS:")
            format_rows(x.default_variant, 'default')
            for v in x.variants:
                if v != x.default_variant:
                    format_rows(v, '')
            format_pg()
            return x
    except grpc._channel._MultiThreadedRendezvous:
        print(f"{resource_type} not found.")

def get_feature_variant_info(stub, name, variant):
    searchNameVariant = metadata_pb2.NameVariant(name=name, variant=variant)
    try:
        for x in stub.GetFeatureVariants(iter([searchNameVariant])):
            return Feature(
                name=x.name,
                value_type=x.type,
                variant=x.variant,
                source=(x.source.name,x.source.variant),
                entity=x.entity,
                owner=x.owner,
                provider=x.provider,
                location=ResourceColumnMapping("","",""),    
                status=ResourceStatus.from_proto(x.status.status),
                description=x.description,
                trainingsets=[(f.name,f.variant) for f in x.trainingsets],
            )
    except grpc._channel._MultiThreadedRendezvous:
        print("Feature variant not found.")

def get_label_variant_info(stub, name, variant):
    searchNameVariant = metadata_pb2.NameVariant(name=name, variant=variant)
    try:
        for x in stub.GetLabelVariants(iter([searchNameVariant])):
            return Label(
                name=x.name,
                source=(x.source.name,x.source.variant),
                value_type=x.type,
                owner=x.owner,
                entity=x.entity,
                provider=x.provider,
                description=x.description,
                location=ResourceColumnMapping("","",""),
                variant=x.variant,
                status=ResourceStatus.from_proto(x.status.status),
                trainingsets=[(f.name,f.variant) for f in x.trainingsets],
            )
    except grpc._channel._MultiThreadedRendezvous:
        print("Label variant not found.")

def source_type_object(source_variant_proto):
    is_transformation = None
    definition = None
    if source_variant_proto.primaryData.table.name:
        definition = PrimaryData(location=SQLTable(name=source_variant_proto.primaryData.table.name))
        is_transformation = "PRIMARY"
    elif x.transformation.SQLTransformation.query:
        definition = SQLTransformation(query=source_variant_proto.transformation.SQLTransformation.query)
        is_transformation = "SQL"
    elif x.transformation.DFTransformation.query:
        definition = DFTransformation(query=source_variant_proto.transformation.DFTransformation.query, inputs=[(f.name, f.variant) for f in source_variant_proto.transformation.DFTransformation.inputs])
        is_transformation="DF"
    return is_transformation, definition

def get_source_variant_info(stub, name, variant):
    searchNameVariant = metadata_pb2.NameVariant(name=name, variant=variant)
    try:
        for x in stub.GetSourceVariants(iter([searchNameVariant])):
            is_transformation, definition = source_type_object(x)
            source = Source(
                name=x.name,
                definition=definition,
                description=x.description,
                variant=x.variant,
                provider=x.provider,
                owner=x.owner,
                status=ResourceStatus.from_proto(x.status.status),
            )
            if is_transformation is not None:
                source.is_transformation = is_transformation
            return source
    except grpc._channel._MultiThreadedRendezvous:
        print("Source variant not found.")

def get_training_set_variant_info(stub, name, variant):
    searchNameVariant = metadata_pb2.NameVariant(name=name, variant=variant)
    try:
        for x in stub.GetTrainingSetVariants(iter([searchNameVariant])):
            return TrainingSet(
                name=x.name,
                variant=x.variant,
                label=(x.label.name,x.label.variant),
                status=ResourceStatus.from_proto(x.status.status),
                description=x.description,
                owner=x.owner,
                schedule=x.schedule,
                features=[(f.name,f.variant) for f in x.features],
                provider=x.provider,
                feature_lags=[(lag.feature, lag.variant) for lag in x.feature_lags],
            )
    except grpc._channel._MultiThreadedRendezvous:
        print("Training set variant not found.")

def get_provider_info(stub, name):
    searchName = metadata_pb2.Name(name=name)
    try:
        for x in stub.GetProviders(iter([searchName])):
            Provider(
                name=x.name,
                description=x.description,
                team=x.team,
                software=x.software,
                provider_type=x.type,
                status=ResourceStatus.from_proto(x.status.status),
                sources=[(f.name,f.variant) for f in x.sources],
                features=[(f.name,f.variant) for f in x.features],
                trainingsets=[(f.name,f.variant) for f in x.trainingsets],
                labels=[(f.name,f.variant) for f in x.labels],
            )
    except grpc._channel._MultiThreadedRendezvous:
        print("Provider not found.")
from .proto import metadata_pb2
from .format import *

def ListNameStatus(stub, resource_type):
    funcDict = {
        "entity": stub.ListEntities,
        "user": stub.ListUsers
    }
    formatRows("NAME", "STATUS")
    for f in funcDict[resource_type](metadata_pb2.Empty()):
        formatRows(f.name, f.status.Status._enum_type.values[f.status.status].name)

def ListNameStatusDesc(stub, resource_type):
    funcDict = {
        "model": stub.ListModels,
        "provider": stub.ListProviders
    }
    formatRows("NAME", "STATUS", "DESCRIPTION")
    for f in funcDict[resource_type](metadata_pb2.Empty()):
        formatRows(f.name, f.status.Status._enum_type.values[f.status.status].name, f.description[:60])

def ListNameVariantStatus(stub, resource_type):
    funcDict = {
        "feature": [stub.ListFeatures, stub.GetFeatureVariants],
        "label": [stub.ListLabels, stub.GetLabelVariants]
    }

    formatRows("NAME", "VARIANT", "STATUS")
    for f in funcDict[resource_type][0](metadata_pb2.Empty()):
        for v in f.variants:
            searchNameVariant = metadata_pb2.NameVariant(name=f.name, variant=v)
            for x in funcDict[resource_type][1](iter([searchNameVariant])):
                if x.variant == f.default_variant:
                    formatRows(f.name, f"{f.default_variant} (default)", f.status.Status._enum_type.values[f.status.status].name)
                else:
                    formatRows(x.name, x.variant, x.status.Status._enum_type.values[x.status.status].name)

def ListNameVariantStatusDesc(stub, resource_type):
    funcDict = {
        "source": [stub.ListSources, stub.GetSourceVariants],
        "training-set": [stub.ListTrainingSets, stub.GetTrainingSetVariants],
        "trainingset": [stub.ListTrainingSets, stub.GetTrainingSetVariants]
    }

    formatRows("NAME", "VARIANT", "STATUS", "DESCRIPTION")
    for f in funcDict[resource_type][0](metadata_pb2.Empty()):
        for v in f.variants:
            searchNameVariant = metadata_pb2.NameVariant(name=f.name, variant=v)
            for x in funcDict[resource_type][1](iter([searchNameVariant])):
                if x.variant == f.default_variant:
                    formatRows(f.name, f"{f.default_variant} (default)", f.status.Status._enum_type.values[f.status.status].name, x.description)
                else:
                    formatRows(x.name, x.variant, x.status.Status._enum_type.values[x.status.status].name, x.description)
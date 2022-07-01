from .proto import metadata_pb2
import grpc
from .proto import metadata_pb2_grpc as ff_grpc
from .format import *

def GetUser(stub, name):
    searchName = metadata_pb2.Name(name=name)
    try:
        for user in stub.GetUsers(iter([searchName])):
            formatRows("USER NAME: ", user.name)
            formatNewPara("")
            formatRows('NAME', 'VARIANT', 'TYPE')
            for f in user.features:
                formatRows(
                    f.name, f.variant, "feature")
            for l in user.labels:
                formatRows(
                    l.name, l.variant, "label")
            for t in user.trainingsets:
                formatRows(
                    t.name, t.variant, "training set")
            for s in user.sources:
                formatRows(
                    s.name, s.variant, "source")
    except grpc._channel._MultiThreadedRendezvous:
        print("User not found.")

def GetResource(stub, resource_type, name):
    funcDict = {
        "feature": stub.GetFeatures,
        "label": stub.GetLabels,
        "source": stub.GetSources,
        "trainingset": stub.GetTrainingSets,
        "training-set": stub.GetTrainingSets,
        "entity": stub.GetEntities,
        "model": stub.GetModels
    }

    searchName = metadata_pb2.Name(name=name)
    try:
        for x in funcDict[resource_type](iter([searchName])):
            formatRows([("NAME: ", x.name),
            ("STATUS: ", x.status.Status._enum_type.values[x.status.status].name)])
            formatNewPara("VARIANTS:")
            formatRows(x.default_variant, 'default')
            for v in x.variants:
                if v != x.default_variant:
                    formatRows(v, '')
    except grpc._channel._MultiThreadedRendezvous:
        print("Feature not found.")

def GetFeatureVariant(stub, name, variant):
    searchNameVariant = metadata_pb2.NameVariant(name=name, variant=variant)
    try:
        for x in stub.GetFeatureVariants(iter([searchNameVariant])):
            formatRows([("NAME: ", x.name), 
            ("VARIANT: ", x.variant), 
            ("TYPE:", x.type), 
            ("ENTITY:", x.entity),
            ("OWNER:", x.owner),
            ("DESCRIPTION:", x.description),
            ("PROVIDER:", x.provider),
            ("STATUS: ", x.status.Status._enum_type.values[x.status.status].name)
            ])
            formatNewPara("SOURCE: ")
            formatRows([("NAME", "VARIANT"), (x.source.name, x.source.variant)])
            formatNewPara("TRAINING SETS:")
            formatRows("NAME", "VARIANT")
            for t in x.trainingsets:
                formatRows(t.name, t.variant)
    except grpc._channel._MultiThreadedRendezvous:
        print("Feature variant not found.")

def GetLabelVariant(stub, name, variant):
    searchNameVariant = metadata_pb2.NameVariant(name=name, variant=variant)
    try:
        for x in stub.GetLabelVariants(iter([searchNameVariant])):
            formatRows([("NAME: ", x.name),
            ("VARIANT: ", x.variant), 
            ("TYPE:", x.type), 
            ("ENTITY:", x.entity), 
            ("OWNER:", x.owner), 
            ("DESCRIPTION:", x.description),
            ("PROVIDER:", x.provider),
            ("STATUS: ", x.status.Status._enum_type.values[x.status.status].name)])
            formatNewPara("SOURCE: ")
            formatRows([("NAME", "VARIANT"), (x.source.name, x.source.variant)])
            formatNewPara("TRAINING SETS:")
            formatRows("NAME", "VARIANT")
            for t in x.trainingsets:
                formatRows(t.name, t.variant)
    except grpc._channel._MultiThreadedRendezvous:
        print("Label variant not found.")

def GetSourceVariant(stub, name, variant):
    searchNameVariant = metadata_pb2.NameVariant(name=name, variant=variant)
    try:
        for x in stub.GetSourceVariants(iter([searchNameVariant])):
            formatRows([("NAME: ", x.name),
            ("VARIANT: ", x.variant), 
            ("OWNER:", x.owner),
            ("DESCRIPTION:", x.description),
            ("PROVIDER:", x.provider),
            ("TABLE:", x.table),
            ("STATUS: ", x.status.Status._enum_type.values[x.status.status].name)])
            formatNewPara("DEFINITION:")
            print("TRANSFORMATION")
            print(x.transformation.SQLTransformation.query)
            formatNewPara("SOURCES")
            formatRows("NAME", "VARIANT")
            for s in x.transformation.SQLTransformation.source:
                formatRows(s.name, s.variant)
            formatNewPara("PRIMARY DATA")
            print(x.primaryData.table.name)
            print("FEATURES:")
            formatRows("NAME", "VARIANT")
            for t in x.features:
                formatRows(t.name, t.variant)
            formatNewPara("LABELS:")
            formatRows("NAME", "VARIANT")
            for t in x.labels:
                formatRows(t.name, t.variant)
            formatNewPara("TRAINING SETS:")
            formatRows("NAME", "VARIANT")
            for t in x.trainingsets:
                formatRows(t.name, t.variant)
    except grpc._channel._MultiThreadedRendezvous:
        print("Source variant not found.")

def GetTrainingSetVariant(stub, name, variant):
    searchNameVariant = metadata_pb2.NameVariant(name=name, variant=variant)
    try:
        for x in stub.GetTrainingSetVariants(iter([searchNameVariant])):
            formatRows([("NAME: ", x.name),
            ("VARIANT: ", x.variant),
            ("OWNER:", x.owner),
            ("DESCRIPTION:", x.description),
            ("PROVIDER:", x.provider),
            ("STATUS: ", x.status.Status._enum_type.values[x.status.status].name)])
            formatNewPara("LABEL: ")
            formatRows([("NAME", "VARIANT"), (x.label.name, x.label.variant)])
            formatNewPara("FEATURES:")
            formatRows("NAME", "VARIANT")
            for f in x.features:
                formatRows(f.name, f.variant)
    except grpc._channel._MultiThreadedRendezvous:
        print("Training set variant not found.")

def GetProvider(stub, name):
    searchName = metadata_pb2.Name(name=name)
    try:
        for x in stub.GetProviders(iter([searchName])):
            formatRows([("NAME: ", x.name),
            ("DESCRIPTION: ", x.description),
            ("TYPE: ", x.type),
            ("SOFTWARE: ", x.software),
            ("TEAM: ", x.team),
            ("STATUS: ", x.status.Status._enum_type.values[x.status.status].name)])
            formatNewPara("SOURCES:")
            formatRows("NAME", "VARIANT")
            for s in x.sources:
                formatRows(s.name, s.variant)
            formatNewPara("FEATURES:")
            formatRows("NAME", "VARIANT")
            for f in x.features:
                formatRows(f.name, f.variant)
            formatNewPara("LABELS:")
            formatRows("NAME", "VARIANT")
            for l in x.labels:
                formatRows(l.name, l.variant)
            formatNewPara("TRAINING SETS:")
            formatRows("NAME", "VARIANT")
            for t in x.trainingsets:
                formatRows(t.name, t.variant)
    except grpc._channel._MultiThreadedRendezvous:
        print("Provider not found.")
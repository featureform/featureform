from .proto import metadata_pb2
import grpc
from .proto import metadata_pb2_grpc as ff_grpc

def GetUser(stub, name):
    searchName = metadata_pb2.Name(name=name)
    try:
        for user in stub.GetUsers(iter([searchName])):
            print("USER NAME: ", user.name)
            print("")
            print("{:<30} {:<35} {:<40}".format('NAME', 'VARIANT', 'TYPE'))
            for f in user.features:
                print("{:<30} {:<35} {:<40}".format(
                    f.name, f.variant, "feature"))
            for l in user.labels:
                print("{:<30} {:<35} {:<40}".format(
                    l.name, l.variant, "label"))
            for t in user.trainingsets:
                print("{:<30} {:<35} {:<40}".format(
                    t.name, t.variant, "training set"))
            for s in user.sources:
                print("{:<30} {:<35} {:<40}".format(
                    s.name, s.variant, "source"))
    except grpc._channel._MultiThreadedRendezvous:
        print("User not found.")

def GetFeature(stub, name):
    searchName = metadata_pb2.Name(name=name)
    try:
        for x in stub.GetFeatures(iter([searchName])):
            print("NAME: ", x.name)
            print("STATUS: ", x.status.Status._enum_type.values[x.status.status].name)
            print("VARIANTS:")
            print("{:<20} {:<25}".format(x.default_variant, 'default'))
            for v in x.variants:
                if v != x.default_variant:
                    print("{:<20} {:<35}".format(v, ''))
    except grpc._channel._MultiThreadedRendezvous:
        print("Feature not found.")

def GetFeatureVariant(stub, name, variant):
    searchNameVariant = metadata_pb2.NameVariant(name=name, variant=variant)
    try:
        for x in stub.GetFeatureVariants(iter([searchNameVariant])):
            print("{:<20} {:<15}".format("NAME: ", x.name))
            print("{:<20} {:<15}".format("VARIANT: ", x.variant))
            print("{:<20} {:<15}".format("TYPE:", x.type))
            print("{:<20} {:<15}".format("ENTITY:", x.entity))
            print("{:<20} {:<15}".format("OWNER:", x.owner))
            print("{:<20} {:<15}".format("DESCRIPTION:", x.description))
            print("{:<20} {:<15}".format("PROVIDER:", x.provider))
            print("{:<20} {:<15}".format("STATUS: ", x.status.Status._enum_type.values[x.status.status].name))
            print("")
            print("SOURCE: ")
            print("{:<30} {:<35}".format("NAME", "VARIANT"))
            print("{:<30} {:<35}".format(x.source.name, x.source.variant))
            print("")
            print("TRAINING SETS:")
            print("{:<30} {:<35}".format("NAME", "VARIANT"))
            for t in x.trainingsets:
                print("{:<30} {:<35}".format(t.name, t.variant))
    except grpc._channel._MultiThreadedRendezvous:
        print("Feature variant not found.")

def GetLabel(stub, name):
    searchName = metadata_pb2.Name(name=name)
    try:
        for x in stub.GetLabels(iter([searchName])):
            print("NAME: ", x.name)
            print("STATUS: ", x.status.Status._enum_type.values[x.status.status].name)
            print("VARIANTS:")
            print("{:<20} {:<25}".format(x.default_variant, 'default'))
            for v in x.variants:
                if v != x.default_variant:
                    print("{:<20} {:<35}".format(v, ''))
    except grpc._channel._MultiThreadedRendezvous:
        print("Label not found.")

def GetLabelVariant(stub, name, variant):
    searchNameVariant = metadata_pb2.NameVariant(name=name, variant=variant)
    try:
        for x in stub.GetLabelVariants(iter([searchNameVariant])):
            print("{:<20} {:<15}".format("NAME: ", x.name))
            print("{:<20} {:<15}".format("VARIANT: ", x.variant))
            print("{:<20} {:<15}".format("TYPE:", x.type))
            print("{:<20} {:<15}".format("ENTITY:", x.entity))
            print("{:<20} {:<15}".format("OWNER:", x.owner))
            print("{:<20} {:<15}".format("DESCRIPTION:", x.description))
            print("{:<20} {:<15}".format("PROVIDER:", x.provider))
            print("{:<20} {:<15}".format("STATUS: ", x.status.Status._enum_type.values[x.status.status].name))
            print("")
            print("SOURCE: ")
            print("{:<30} {:<35}".format("NAME", "VARIANT"))
            print("{:<30} {:<35}".format(x.source.name, x.source.variant))
            print("")
            print("TRAINING SETS:")
            print("{:<30} {:<35}".format("NAME", "VARIANT"))
            for t in x.trainingsets:
                print("{:<30} {:<35}".format(t.name, t.variant))
    except grpc._channel._MultiThreadedRendezvous:
        print("Label variant not found.")

def GetSource(stub, name):
    searchName = metadata_pb2.Name(name=name)
    try:
        for x in stub.GetSources(iter([searchName])):
            print("NAME: ", x.name)
            print("STATUS: ", x.status.Status._enum_type.values[x.status.status].name)
            print("VARIANTS:")
            print("{:<20} {:<25}".format(x.default_variant, 'default'))
            for v in x.variants:
                if v != x.default_variant:
                    print("{:<20} {:<35}".format(v, ''))
    except grpc._channel._MultiThreadedRendezvous:
        print("Source not found.")

def GetSourceVariant(stub, name, variant):
    searchNameVariant = metadata_pb2.NameVariant(name=name, variant=variant)
    try:
        for x in stub.GetSourceVariants(iter([searchNameVariant])):
            print("{:<20} {:<15}".format("NAME: ", x.name))
            print("{:<20} {:<15}".format("VARIANT: ", x.variant))
            print("{:<20} {:<15}".format("OWNER:", x.owner))
            print("{:<20} {:<15}".format("DESCRIPTION:", x.description))
            print("{:<20} {:<15}".format("PROVIDER:", x.provider))
            print("{:<20} {:<15}".format("TABLE:", x.table))
            print("{:<20} {:<15}".format("STATUS: ", x.status.Status._enum_type.values[x.status.status].name))
            print("")
            print("DEFINITION:")
            print("TRANSFORMATION")
            print(x.transformation.SQLTransformation.query)
            print("")
            print("SOURCES")
            print("{:<30} {:<35}".format("NAME", "VARIANT"))
            for s in x.transformation.SQLTransformation.source:
                print("{:<30} {:<35}".format(s.name, s.variant))
            print("")
            print("PRIMARY DATA")
            print(x.primaryData.table.name)
            print("FEATURES:")
            print("{:<30} {:<35}".format("NAME", "VARIANT"))
            for t in x.features:
                print("{:<30} {:<35}".format(t.name, t.variant))
            print("")
            print("LABELS:")
            print("{:<30} {:<35}".format("NAME", "VARIANT"))
            for t in x.labels:
                print("{:<30} {:<35}".format(t.name, t.variant))
            print("")
            print("TRAINING SETS:")
            print("{:<30} {:<35}".format("NAME", "VARIANT"))
            for t in x.trainingsets:
                print("{:<30} {:<35}".format(t.name, t.variant))
    except grpc._channel._MultiThreadedRendezvous:
        print("Source variant not found.")

def GetTrainingSet(stub, name):
    searchName = metadata_pb2.Name(name=name)
    try:
        for x in stub.GetTrainingSets(iter([searchName])):
            print("NAME: ", x.name)
            print("STATUS: ", x.status.Status._enum_type.values[x.status.status].name)
            print("VARIANTS:")
            print("{:<20} {:<25}".format(x.default_variant, 'default'))
            for v in x.variants:
                if v != x.default_variant:
                    print("{:<20} {:<35}".format(v, ''))
    except grpc._channel._MultiThreadedRendezvous:
        print("Training set not found.")

def GetTrainingSetVariant(stub, name, variant):
    searchNameVariant = metadata_pb2.NameVariant(name=name, variant=variant)
    try:
        for x in stub.GetTrainingSetVariants(iter([searchNameVariant])):
            print("{:<20} {:<15}".format("NAME: ", x.name))
            print("{:<20} {:<15}".format("VARIANT: ", x.variant))
            print("{:<20} {:<15}".format("OWNER:", x.owner))
            print("{:<20} {:<15}".format("DESCRIPTION:", x.description))
            print("{:<20} {:<15}".format("PROVIDER:", x.provider))
            print("{:<20} {:<15}".format("STATUS: ", x.status.Status._enum_type.values[x.status.status].name))
            print("")
            print("LABEL: ")
            print("{:<30} {:<35}".format("NAME", "VARIANT"))
            print("{:<30} {:<35}".format(x.label.name, x.label.variant))
            print("")
            print("FEATURES:")
            print("{:<30} {:<35}".format("NAME", "VARIANT"))
            for f in x.features:
                print("{:<30} {:<35}".format(f.name, f.variant))
    except grpc._channel._MultiThreadedRendezvous:
        print("Training set variant not found.")

def GetProvider(stub, name):
    searchName = metadata_pb2.Name(name=name)
    try:
        for x in stub.GetProviders(iter([searchName])):
            print("{:<20} {:<15}".format("NAME: ", x.name))
            print("{:<20} {:<15}".format("DESCRIPTION: ", x.description))
            print("{:<20} {:<15}".format("TYPE: ", x.type))
            print("{:<20} {:<15}".format("SOFTWARE: ", x.software))
            print("{:<20} {:<15}".format("TEAM: ", x.team))
            print("{:<20} {:<15}".format("STATUS: ", x.status.Status._enum_type.values[x.status.status].name))
            print("")
            print("SOURCES:")
            print("{:<30} {:<35}".format("NAME", "VARIANT"))
            for s in x.sources:
                print("{:<30} {:<35}".format(s.name, s.variant))
            print("")
            print("FEATURES:")
            print("{:<30} {:<35}".format("NAME", "VARIANT"))
            for f in x.features:
                print("{:<30} {:<35}".format(f.name, f.variant))
            print("")
            print("LABELS:")
            print("{:<30} {:<35}".format("NAME", "VARIANT"))
            for l in x.labels:
                print("{:<30} {:<35}".format(l.name, l.variant))
            print("")
            print("TRAINING SETS:")
            print("{:<30} {:<35}".format("NAME", "VARIANT"))
            for t in x.trainingsets:
                print("{:<30} {:<35}".format(t.name, t.variant))
    except grpc._channel._MultiThreadedRendezvous:
        print("Provider not found.")

def GetEntity(stub, name):
    searchName = metadata_pb2.Name(name=name)
    try:
        for x in stub.GetEntities(iter([searchName])):
            print("{:<20} {:<15}".format("NAME: ", x.name))
            print("{:<20} {:<15}".format("STATUS: ", x.status.Status._enum_type.values[x.status.status].name))
            print("VARIANTS:")
            print("{:<20} {:<25}".format(x.default_variant, 'default'))
            for v in x.variants:
                if v != x.default_variant:
                    print("{:<20} {:<35}".format(v, ''))
    except grpc._channel._MultiThreadedRendezvous:
        print("Entity not found.")

def GetModel(stub, name):
    searchName = metadata_pb2.Name(name=name)
    try:
        for x in stub.GetModels(iter([searchName])):
            print("{:<20} {:<15}".format("NAME: ", x.name))
            print("{:<20} {:<15}".format("STATUS: ", x.status.Status._enum_type.values[x.status.status].name))
            print("VARIANTS:")
            print("{:<20} {:<25}".format(x.default_variant, 'default'))
            for v in x.variants:
                if v != x.default_variant:
                    print("{:<20} {:<35}".format(v, ''))
    except grpc._channel._MultiThreadedRendezvous:
        print("Model not found.")
from .proto import metadata_pb2
from .format import *

def list_name_status(stub, resource_type):
    stub_list_functions = {
        "entity": stub.ListEntities,
        "user": stub.ListUsers
    }
    format_rows("NAME", "STATUS")
    res = []
    for f in stub_list_functions[resource_type](metadata_pb2.Empty()):
        format_rows(f.name, f.status.Status._enum_type.values[f.status.status].name)
        res.append(f)
    return res

def list_name_status_desc(stub, resource_type):
    stub_list_functions = {
        "model": stub.ListModels,
        "provider": stub.ListProviders
    }
    format_rows("NAME", "STATUS", "DESCRIPTION")
    res = []
    for f in stub_list_functions[resource_type](metadata_pb2.Empty()):
        format_rows(f.name, f.status.Status._enum_type.values[f.status.status].name, f.description[:60])
        res.append(f)
    return res

def list_name_variant_status(stub, resource_type):
    stub_list_functions = {
        "feature": [stub.ListFeatures, stub.GetFeatureVariants],
        "label": [stub.ListLabels, stub.GetLabelVariants]
    }

    format_rows("NAME", "VARIANT", "STATUS")
    res = []
    for f in stub_list_functions[resource_type][0](metadata_pb2.Empty()):
        res.append(f)
        for v in f.variants:
            searchNameVariant = metadata_pb2.NameVariant(name=f.name, variant=v)
            for x in stub_list_functions[resource_type][1](iter([searchNameVariant])):
                if x.variant == f.default_variant:
                    format_rows(f.name, f"{f.default_variant} (default)", f.status.Status._enum_type.values[f.status.status].name)
                else:
                    format_rows(x.name, x.variant, x.status.Status._enum_type.values[x.status.status].name)
    return res

def list_name_variant_status_desc(stub, resource_type):
    stub_list_functions = {
        "source": [stub.ListSources, stub.GetSourceVariants],
        "training-set": [stub.ListTrainingSets, stub.GetTrainingSetVariants],
        "trainingset": [stub.ListTrainingSets, stub.GetTrainingSetVariants]
    }

    format_rows("NAME", "VARIANT", "STATUS", "DESCRIPTION")
    res = []
    for f in stub_list_functions[resource_type][0](metadata_pb2.Empty()):
        res.append(f)
        for v in f.variants:
            searchNameVariant = metadata_pb2.NameVariant(name=f.name, variant=v)
            for x in stub_list_functions[resource_type][1](iter([searchNameVariant])):
                if x.variant == f.default_variant:
                    format_rows(f.name, f"{f.default_variant} (default)", f.status.Status._enum_type.values[f.status.status].name, x.description)
                else:
                    format_rows(x.name, x.variant, x.status.Status._enum_type.values[x.status.status].name, x.description)
    return res
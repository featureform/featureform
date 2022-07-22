from .format import *
from .sqlite_metadata import *

def get_user_info_local(name):
    db = SQLiteMetadata()
    users_list = db.getVariantResource("users", "name", name)
    user = users_list[0]
    format_rows("USER NAME: ", user["name"])
    format_rows("TYPE: ", user["type"])
    format_rows("STATUS: ", user["status"])
    return user

def get_entity_info_local(name):
    db = SQLiteMetadata()
    entity_list = db.getVariantResource("entities", "name", name)
    entity = entity_list[0]
    try:
        feature_list = db.getVariantResource("feature_variant", "entity", name)
    except ValueError:
        pass
    returned_features_list = [{ "name": f["featureName"], "variant": f["variantName"]} for f in feature_list]

    try:
        label_list = db.getVariantResource("label_variant", "entity", name)
    except ValueError:
        pass
    returned_labels_list = [{ "name": l["labelName"], "variant": l["variantName"]} for l in label_list]

    training_set_list = set()
    for f in returned_features_list:
        try:
            training_set_features_query_list = db.getNameVariant("training_set_features", "featureName", f["name"], "featureVariant", f["variant"])
        except ValueError:
            pass
        for t in training_set_features_query_list:
            training_set_list.add(t)
    returned_training_sets_list = [{ "name": t["trainingSetName"], "variant": t["trainingSetVariant"]} for t in training_set_list]
    returned_entity = {
        "name": name,
        "status": entity["status"],
        "features": returned_features_list,
        "labels": returned_labels_list,
        "trainingsets": returned_training_sets_list
    }

    format_rows("ENTITY NAME: ", returned_entity["name"]),
    ("STATUS: ", returned_entity["status"])
    format_new_para("")
    format_rows('NAME', 'VARIANT', 'TYPE')
    for f in returned_entity["features"]:
        format_rows(
            f["name"], f["variant"], "feature")
    for l in returned_entity["labels"]:
        format_rows(
            l["name"], l["variant"], "label")
    for t in returned_entity["trainingsets"]:
        format_rows(
            t["name"], t["variant"], "training set")
    format_new_para("")
    return returned_entity

def get_resource_info_local(resource_type, name):
    get_query_names = {
        "feature": ["features", "feature_variant", "featureName"],
        "label": ["labels", "label_variant", "labelName"],
        "source": ["sources", "source_variant", "sourceName"],
        "trainingset": ["training_sets", "training_set_variant", "trainingSetName"],
        "training-set": ["training_sets", "training_set_variant", "trainingSetName"]
    }

    db = SQLiteMetadata()
    resource_list = db.getVariantResource(get_query_names[resource_type][0], "name", name)
    resource = resource_list[0]
    variants_list = db.getVariantResource(get_query_names[resource_type][1], get_query_names[resource_type][2], name)
   
    returned_resource_list = {
        "name": resource["name"],
        "default_variant": resource["defaultVariant"],
        "variants": [v["variantName"] for v in variants_list]
    }
   
    format_rows("NAME: ", returned_resource_list["name"])
    if "status" in resource:
        format_rows("STATUS: ", resource["status"])
    format_new_para("VARIANTS:")
    format_rows(returned_resource_list["default_variant"], 'default')
    for v in returned_resource_list["variants"]:
        if v != returned_resource_list["default_variant"]:
            format_rows(v, '')
    format_new_para("")
    return returned_resource_list

# def get_feature_variant_info(name, variant):
#     searchNameVariant = metadata_pb2.NameVariant(name=name, variant=variant)
#     try:
#         for x in etFeatureVariants(iter([searchNameVariant])):
#             format_rows([("NAME: ", x.name), 
#             ("VARIANT: ", x.variant), 
#             ("TYPE:", x.type), 
#             ("ENTITY:", x.entity),
#             ("OWNER:", x.owner),
#             ("DESCRIPTION:", x.description),
#             ("PROVIDER:", x.provider),
#             ("STATUS: ", x.status.Status._enum_type.values[x.status.status].name)
#             ])
#             format_new_para("SOURCE: ")
#             format_rows([("NAME", "VARIANT"), (x.source.name, x.source.variant)])
#             format_new_para("TRAINING SETS:")
#             format_rows("NAME", "VARIANT")
#             for t in x.trainingsets:
#                 format_rows(t.name, t.variant)
#             format_new_para("")
#             return x
#     except grpc._channel._MultiThreadedRendezvous:
#         print("Feature variant not found.")

# def get_label_variant_info(name, variant):
#     searchNameVariant = metadata_pb2.NameVariant(name=name, variant=variant)
#     try:
#         for x in etLabelVariants(iter([searchNameVariant])):
#             format_rows([("NAME: ", x.name),
#             ("VARIANT: ", x.variant), 
#             ("TYPE:", x.type), 
#             ("ENTITY:", x.entity), 
#             ("OWNER:", x.owner), 
#             ("DESCRIPTION:", x.description),
#             ("PROVIDER:", x.provider),
#             ("STATUS: ", x.status.Status._enum_type.values[x.status.status].name)])
#             format_new_para("SOURCE: ")
#             format_rows([("NAME", "VARIANT"), (x.source.name, x.source.variant)])
#             format_new_para("TRAINING SETS:")
#             format_rows("NAME", "VARIANT")
#             for t in x.trainingsets:
#                 format_rows(t.name, t.variant)
#             format_new_para("")
#             return x
#     except grpc._channel._MultiThreadedRendezvous:
#         print("Label variant not found.")

# def get_source_variant_info(name, variant):
#     searchNameVariant = metadata_pb2.NameVariant(name=name, variant=variant)
#     try:
#         for x in etSourceVariants(iter([searchNameVariant])):
#             format_rows([("NAME: ", x.name),
#             ("VARIANT: ", x.variant), 
#             ("OWNER:", x.owner),
#             ("DESCRIPTION:", x.description),
#             ("PROVIDER:", x.provider),
#             ("TABLE:", x.table),
#             ("STATUS: ", x.status.Status._enum_type.values[x.status.status].name)])
#             format_new_para("DEFINITION:")
#             print("TRANSFORMATION")
#             print(x.transformation.SQLTransformation.query)
#             format_new_para("SOURCES")
#             format_rows("NAME", "VARIANT")
#             for s in x.transformation.SQLTransformation.source:
#                 format_rows(s.name, s.variant)
#             format_new_para("PRIMARY DATA")
#             print(x.primaryData.table.name)
#             print("FEATURES:")
#             format_rows("NAME", "VARIANT")
#             for t in x.features:
#                 format_rows(t.name, t.variant)
#             format_new_para("LABELS:")
#             format_rows("NAME", "VARIANT")
#             for t in x.labels:
#                 format_rows(t.name, t.variant)
#             format_new_para("TRAINING SETS:")
#             format_rows("NAME", "VARIANT")
#             for t in x.trainingsets:
#                 format_rows(t.name, t.variant)
#             format_new_para("")
#             return x
#     except grpc._channel._MultiThreadedRendezvous:
#         print("Source variant not found.")

# def get_training_set_variant_info(name, variant):
#     searchNameVariant = metadata_pb2.NameVariant(name=name, variant=variant)
#     try:
#         for x in etTrainingSetVariants(iter([searchNameVariant])):
#             format_rows([("NAME: ", x.name),
#             ("VARIANT: ", x.variant),
#             ("OWNER:", x.owner),
#             ("DESCRIPTION:", x.description),
#             ("PROVIDER:", x.provider),
#             ("STATUS: ", x.status.Status._enum_type.values[x.status.status].name)])
#             format_new_para("LABEL: ")
#             format_rows([("NAME", "VARIANT"), (x.label.name, x.label.variant)])
#             format_new_para("FEATURES:")
#             format_rows("NAME", "VARIANT")
#             for f in x.features:
#                 format_rows(f.name, f.variant)
#             format_new_para("")
#             return x
#     except grpc._channel._MultiThreadedRendezvous:
#         print("Training set variant not found.")

# def get_provider_info(name):
#     searchName = metadata_pb2.Name(name=name)
#     try:
#         for x in etProviders(iter([searchName])):
#             format_rows([("NAME: ", x.name),
#             ("DESCRIPTION: ", x.description),
#             ("TYPE: ", x.type),
#             ("SOFTWARE: ", x.software),
#             ("TEAM: ", x.team),
#             ("STATUS: ", x.status.Status._enum_type.values[x.status.status].name)])
#             format_new_para("SOURCES:")
#             format_rows("NAME", "VARIANT")
#             for s in x.sources:
#                 format_rows(s.name, s.variant)
#             format_new_para("FEATURES:")
#             format_rows("NAME", "VARIANT")
#             for f in x.features:
#                 format_rows(f.name, f.variant)
#             format_new_para("LABELS:")
#             format_rows("NAME", "VARIANT")
#             for l in x.labels:
#                 format_rows(l.name, l.variant)
#             format_new_para("TRAINING SETS:")
#             format_rows("NAME", "VARIANT")
#             for t in x.trainingsets:
#                 format_rows(t.name, t.variant)
#             format_new_para("")
#             return x
#     except grpc._channel._MultiThreadedRendezvous:
#         print("Provider not found.")
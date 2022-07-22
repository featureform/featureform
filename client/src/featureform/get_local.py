from .format import *
from .sqlite_metadata import *

def get_user_info(name):
    db = SQLiteMetadata()
    users_list = db.getVariantResource("users", "name", name)
    if users_list == []:
        print("User not found.")
        return
    user = users_list[0]
    format_rows("USER NAME: ", user[0])
    format_rows("TYPE: ", user[1])
    format_rows("STATUS: ", user[2])
    return user

def get_entity_info(name):
    db = SQLiteMetadata()
    entity_list = db.getVariantResource("users", "name", name)
    if entity_list == []:
        print("Entity not found.")
        return
    entity = entity_list[0]
    feature_list = db.getVariantResource("feature_variant", "entity", name)
    returned_features_list = [{ "name": f[3], "variant": f[7]} for f in feature_list]

    label_list = db.getVariantResource("label_variant", "entity", name)
    returned_labels_list = [{ "name": f[3], "variant": f[7]} for f in label_list]

    training_set_list = db.getVariantResource("training_set_variant", "entity", name)
    returned_training_sets_list = [{ "name": f[2], "variant": f[4]} for f in training_set_list]
    returned_entity = {
        "name": name,
        "status": entity[3],
        "features": returned_features_list,
        "labels": returned_labels_list,
        "trainingsets": returned_training_sets_list
    }

    format_rows("ENTITY NAME: ", returned_entity.name),
    ("STATUS: ", returned_entity.status)
    format_new_para("")
    format_rows('NAME', 'VARIANT', 'TYPE')
    for f in returned_entity.features:
        format_rows(
            f.name, f.variant, "feature")
    for l in returned_entity.labels:
        format_rows(
            l.name, l.variant, "label")
    for t in returned_entity.trainingsets:
        format_rows(
            t.name, t.variant, "training set")
    format_new_para("")
    return returned_entity

def get_resource_info(resource_type, name):
    stub_get_functions = {
        "feature": etFeatures,
        "label": etLabels,
        "source": etSources,
        "trainingset": etTrainingSets,
        "training-set": etTrainingSets,
        "model": etModels
    }

    searchName = metadata_pb2.Name(name=name)
    try:
        for x in stub_get_functions[resource_type](iter([searchName])):
            format_rows([("NAME: ", x.name),
            ("STATUS: ", x.status.Status._enum_type.values[x.status.status].name)])
            format_new_para("VARIANTS:")
            format_rows(x.default_variant, 'default')
            for v in x.variants:
                if v != x.default_variant:
                    format_rows(v, '')
            format_new_para("")
            return x
    except grpc._channel._MultiThreadedRendezvous:
        print(f"{resource_type} not found.")

def get_feature_variant_info(name, variant):
    searchNameVariant = metadata_pb2.NameVariant(name=name, variant=variant)
    try:
        for x in etFeatureVariants(iter([searchNameVariant])):
            format_rows([("NAME: ", x.name), 
            ("VARIANT: ", x.variant), 
            ("TYPE:", x.type), 
            ("ENTITY:", x.entity),
            ("OWNER:", x.owner),
            ("DESCRIPTION:", x.description),
            ("PROVIDER:", x.provider),
            ("STATUS: ", x.status.Status._enum_type.values[x.status.status].name)
            ])
            format_new_para("SOURCE: ")
            format_rows([("NAME", "VARIANT"), (x.source.name, x.source.variant)])
            format_new_para("TRAINING SETS:")
            format_rows("NAME", "VARIANT")
            for t in x.trainingsets:
                format_rows(t.name, t.variant)
            format_new_para("")
            return x
    except grpc._channel._MultiThreadedRendezvous:
        print("Feature variant not found.")

def get_label_variant_info(name, variant):
    searchNameVariant = metadata_pb2.NameVariant(name=name, variant=variant)
    try:
        for x in etLabelVariants(iter([searchNameVariant])):
            format_rows([("NAME: ", x.name),
            ("VARIANT: ", x.variant), 
            ("TYPE:", x.type), 
            ("ENTITY:", x.entity), 
            ("OWNER:", x.owner), 
            ("DESCRIPTION:", x.description),
            ("PROVIDER:", x.provider),
            ("STATUS: ", x.status.Status._enum_type.values[x.status.status].name)])
            format_new_para("SOURCE: ")
            format_rows([("NAME", "VARIANT"), (x.source.name, x.source.variant)])
            format_new_para("TRAINING SETS:")
            format_rows("NAME", "VARIANT")
            for t in x.trainingsets:
                format_rows(t.name, t.variant)
            format_new_para("")
            return x
    except grpc._channel._MultiThreadedRendezvous:
        print("Label variant not found.")

def get_source_variant_info(name, variant):
    searchNameVariant = metadata_pb2.NameVariant(name=name, variant=variant)
    try:
        for x in etSourceVariants(iter([searchNameVariant])):
            format_rows([("NAME: ", x.name),
            ("VARIANT: ", x.variant), 
            ("OWNER:", x.owner),
            ("DESCRIPTION:", x.description),
            ("PROVIDER:", x.provider),
            ("TABLE:", x.table),
            ("STATUS: ", x.status.Status._enum_type.values[x.status.status].name)])
            format_new_para("DEFINITION:")
            print("TRANSFORMATION")
            print(x.transformation.SQLTransformation.query)
            format_new_para("SOURCES")
            format_rows("NAME", "VARIANT")
            for s in x.transformation.SQLTransformation.source:
                format_rows(s.name, s.variant)
            format_new_para("PRIMARY DATA")
            print(x.primaryData.table.name)
            print("FEATURES:")
            format_rows("NAME", "VARIANT")
            for t in x.features:
                format_rows(t.name, t.variant)
            format_new_para("LABELS:")
            format_rows("NAME", "VARIANT")
            for t in x.labels:
                format_rows(t.name, t.variant)
            format_new_para("TRAINING SETS:")
            format_rows("NAME", "VARIANT")
            for t in x.trainingsets:
                format_rows(t.name, t.variant)
            format_new_para("")
            return x
    except grpc._channel._MultiThreadedRendezvous:
        print("Source variant not found.")

def get_training_set_variant_info(name, variant):
    searchNameVariant = metadata_pb2.NameVariant(name=name, variant=variant)
    try:
        for x in etTrainingSetVariants(iter([searchNameVariant])):
            format_rows([("NAME: ", x.name),
            ("VARIANT: ", x.variant),
            ("OWNER:", x.owner),
            ("DESCRIPTION:", x.description),
            ("PROVIDER:", x.provider),
            ("STATUS: ", x.status.Status._enum_type.values[x.status.status].name)])
            format_new_para("LABEL: ")
            format_rows([("NAME", "VARIANT"), (x.label.name, x.label.variant)])
            format_new_para("FEATURES:")
            format_rows("NAME", "VARIANT")
            for f in x.features:
                format_rows(f.name, f.variant)
            format_new_para("")
            return x
    except grpc._channel._MultiThreadedRendezvous:
        print("Training set variant not found.")

def get_provider_info(name):
    searchName = metadata_pb2.Name(name=name)
    try:
        for x in etProviders(iter([searchName])):
            format_rows([("NAME: ", x.name),
            ("DESCRIPTION: ", x.description),
            ("TYPE: ", x.type),
            ("SOFTWARE: ", x.software),
            ("TEAM: ", x.team),
            ("STATUS: ", x.status.Status._enum_type.values[x.status.status].name)])
            format_new_para("SOURCES:")
            format_rows("NAME", "VARIANT")
            for s in x.sources:
                format_rows(s.name, s.variant)
            format_new_para("FEATURES:")
            format_rows("NAME", "VARIANT")
            for f in x.features:
                format_rows(f.name, f.variant)
            format_new_para("LABELS:")
            format_rows("NAME", "VARIANT")
            for l in x.labels:
                format_rows(l.name, l.variant)
            format_new_para("TRAINING SETS:")
            format_rows("NAME", "VARIANT")
            for t in x.trainingsets:
                format_rows(t.name, t.variant)
            format_new_para("")
            return x
    except grpc._channel._MultiThreadedRendezvous:
        print("Provider not found.")
from cProfile import label
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
        features_list = db.getVariantResource("feature_variant", "entity", name)
    except ValueError:
        features_list = []
    returned_features_list = [{ "name": f["featureName"], "variant": f["variantName"]} for f in features_list]

    try:
        labels_list = db.getVariantResource("label_variant", "entity", name)
    except ValueError:
        labels_list = []
    returned_labels_list = [{ "name": l["labelName"], "variant": l["variantName"]} for l in labels_list]

    training_set_list = set()
    for f in returned_features_list:
        try:
            training_set_features_query_list = db.getNameVariant("training_set_features", "featureName", f["name"], "featureVariant", f["variant"])
        except ValueError:
            training_set_features_query_list = []
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
    try:
        variants_list = db.getVariantResource(get_query_names[resource_type][1], get_query_names[resource_type][2], name)
    except ValueError:
        variants_list = []

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

def get_feature_variant_info_local(name, variant):
    db = SQLiteMetadata()
    features_list = db.getNameVariant("feature_variant", "featureName", name, "variantName", variant)
    feature = features_list[0]
    try:
        training_set_list = db.getNameVariant("training_set_features", "featureName", feature["featureName"], "featureVariant", feature["variantName"])
    except ValueError:
        training_set_list = []
    returned_training_sets_list = [{ "name": t["trainingSetName"], "variant": t["trainingSetVariant"]} for t in training_set_list]
    returned_feature = {
        "name": feature["featureName"],
        "variant": feature["variantName"],
        "type": feature["dataType"],
        "entity": feature["entity"],
        "owner": feature["owner"],
        "description": feature["description"],
        "provider": feature["provider"],
        "status": feature["status"],
        "source": { 
            "name": feature["sourceName"],
            "variant": feature["sourceVariant"]
        },
        "trainingsets": returned_training_sets_list
    }
    format_rows([("NAME: ", returned_feature["name"]), 
    ("VARIANT: ", returned_feature["variant"]), 
    ("TYPE:", returned_feature["type"]), 
    ("ENTITY:", returned_feature["entity"]),
    ("OWNER:", returned_feature["owner"]),
    ("DESCRIPTION:", returned_feature["description"]),
    ("PROVIDER:", returned_feature["provider"]),
    ("STATUS: ", returned_feature["status"])
    ])
    format_new_para("SOURCE: ")
    format_rows([("NAME", "VARIANT"), (returned_feature["source"]["name"], returned_feature["source"]["variant"])])
    format_new_para("TRAINING SETS:")
    format_rows("NAME", "VARIANT")
    for t in returned_feature["trainingsets"]:
        format_rows(t["name"], t["variant"])
    format_new_para("")
    return returned_feature

def get_label_variant_info_local(name, variant):
    db = SQLiteMetadata()
    labels_list = db.getNameVariant("label_variant", "labelName", name, "variantName", variant)
    label = labels_list[0]
    try:
        training_set_list = db.getNameVariant("training_set_variant", "labelName", label["labelName"], "labelVariant", label["variantName"])
    except ValueError:
        training_set_list = []
    returned_training_sets_list = [{ "name": t["trainingSetName"], "variant": t["variantName"]} for t in training_set_list]
    returned_label = {
        "name": label["labelName"],
        "variant": label["variantName"],
        "type": label["dataType"],
        "entity": label["entity"],
        "owner": label["owner"],
        "description": label["description"],
        "provider": label["provider"],
        "status": label["status"],
        "source": { 
            "name": label["sourceName"],
            "variant": label["sourceVariant"]
        },
        "trainingsets": returned_training_sets_list
    }
    format_rows([("NAME: ", returned_label["name"]), 
    ("VARIANT: ", returned_label["variant"]), 
    ("TYPE:", returned_label["type"]), 
    ("ENTITY:", returned_label["entity"]),
    ("OWNER:", returned_label["owner"]),
    ("DESCRIPTION:", returned_label["description"]),
    ("PROVIDER:", returned_label["provider"]),
    ("STATUS: ", returned_label["status"])
    ])
    format_new_para("SOURCE: ")
    format_rows([("NAME", "VARIANT"), (returned_label["source"]["name"], returned_label["source"]["variant"])])
    format_new_para("TRAINING SETS:")
    format_rows("NAME", "VARIANT")
    for t in returned_label["trainingsets"]:
        format_rows(t["name"], t["variant"])
    format_new_para("")
    return returned_label

def get_source_variant_info_local(name, variant):
    db = SQLiteMetadata()
    source_list = db.getNameVariant("source_variant", "name", name, "variant", variant)
    source = source_list[0]

    try:
        features_list = db.getNameVariant("feature_variant", "sourceName", name, "sourceVariant", variant)
    except ValueError:
        features_list = []

    returned_features_list = [{ "name": f["featureName"], "variant": f["variantName"]} for f in features_list]
    
    try:
        labels_list = db.getNameVariant("label_variant", "sourceName", name, "sourceVariant", variant)
    except ValueError:
        labels_list = []
    returned_labels_list = [{ "name": f["labelName"], "variant": f["variantName"]} for f in labels_list]
    
    training_sets_list = set()
    for f in returned_features_list:
        try:
            training_set_features_query_list = db.getNameVariant("training_set_features", "featureName", f["name"], "featureVariant", f["variant"])
        except ValueError:
            pass
        for t in training_set_features_query_list:
            training_sets_list.add(t)

    returned_training_sets_list = [{ "name": f["trainingSetName"], "variant": f["trainingSetVariant"]} for f in training_sets_list]
    
    training_sets_list = set()

    for l in returned_labels_list:
        try:
            training_set_features_query_list = db.getNameVariant("training_set_variant", "labelName", l["name"], "labelVariant", l["variant"])
        except ValueError:
            pass
        for t in training_set_features_query_list:
            training_sets_list.add(t)
    
    returned_training_sets_list += [{ "name": f["trainingSetName"], "variant": f["variantName"]} for f in training_sets_list]
    
    returned_source = {
        "name": source["name"],
        "variant": source["variant"],
        "owner": source["owner"],
        "description": source["description"],
        "provider": source["provider"],
        "status": source["status"],
        "definition": source["definition"],
        "features": returned_features_list,
        "labels": returned_labels_list,
        "trainingsets": returned_training_sets_list
    }

    format_rows([("NAME: ", returned_source["name"]),
    ("VARIANT: ", returned_source["variant"]), 
    ("OWNER:", returned_source["owner"]),
    ("DESCRIPTION:", returned_source["description"]),
    ("PROVIDER:", returned_source["provider"]),
    ("STATUS: ", returned_source["status"])])
    format_new_para("DEFINITION:")
    print(returned_source["definition"])
    print("FEATURES:")
    format_rows("NAME", "VARIANT")
    for f in returned_source["features"]:
        format_rows(f["name"], f["variant"])
    format_new_para("LABELS:")
    format_rows("NAME", "VARIANT")
    for l in returned_source["labels"]:
        format_rows(l["name"], l["variant"])
    format_new_para("TRAINING SETS:")
    format_rows("NAME", "VARIANT")
    for t in returned_source["trainingsets"]:
        format_rows(t["name"], t["variant"])
    format_new_para("")
    return returned_source

def get_training_set_variant_info_local(name, variant):
    db = SQLiteMetadata()
    training_sets_list = db.getNameVariant("training_set_variant", "trainingSetName", name, "variantName", variant)
    training_set = training_sets_list[0]

    try:
        features_list = db.getNameVariant("training_set_features", "trainingSetName", name, "trainingSetVariant", variant)
    except ValueError:
        features_list = []

    returned_features_list = [{ "name": f["featureName"], "variant": f["featureVariant"]} for f in features_list]

    returned_training_set = {
        "name": training_set["trainingSetName"],
        "variant": training_set["variantName"],
        "owner": training_set["owner"],
        "description": training_set["description"],
        "status": training_set["status"],
        "label": {
            "name": training_set["labelName"],
            "variant": training_set["variantName"]
        },
        "features": returned_features_list
    }
    format_rows([("NAME: ", returned_training_set["name"]),
    ("VARIANT: ", returned_training_set["variant"]),
    ("OWNER:", returned_training_set["owner"]),
    ("DESCRIPTION:", returned_training_set["description"]),
    ("STATUS: ", returned_training_set["status"])])
    format_new_para("LABEL: ")
    format_rows([("NAME", "VARIANT"), (returned_training_set["label"]["name"], returned_training_set["label"]["variant"])])
    format_new_para("FEATURES:")
    format_rows("NAME", "VARIANT")
    for f in returned_training_set["features"]:
        format_rows(f["name"], f["variant"])
    format_new_para("")
    return returned_training_set

def get_provider_info(name):

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

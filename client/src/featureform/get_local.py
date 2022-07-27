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
    
    returned_features_list = get_related_resources("feature_variant", 
                                                    column1="entity", 
                                                    name=name)
    returned_labels_list = get_related_resources("label_variant", 
                                                    column1="entity", 
                                                    name=name)

    training_set_list = set()
    for f in returned_features_list:
        training_set_features_query_list = get_related_resources("training_set_features", 
                                                                name_col="trainingSetName", 
                                                                var_col="trainingSetVariant", 
                                                                column1="featureName", 
                                                                name=f["name"], 
                                                                column2="featureVariant", 
                                                                variant=f["variant"], 
                                                                format=False)
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
        "feature": ["features", "feature_variant", "name", "variant"],
        "label": ["labels", "label_variant", "name", "variant"],
        "source": ["sources", "source_variant", "name", "variant"],
        "trainingset": ["training_sets", "training_set_variant", "trainingSetName", "variantName"],
        "training-set": ["training_sets", "training_set_variant", "trainingSetName", "variantName"]
    }

    resource_list = get_related_resources(get_query_names[resource_type][0], 
                                            column1="name", 
                                            name=name, 
                                            format=False)
    resource = resource_list[0]

    variants_list = get_related_resources(get_query_names[resource_type][1], 
                                            column1=get_query_names[resource_type][2], 
                                            name=name, 
                                            format=False)

    returned_resource_list = {
        "name": resource["name"],
        "default_variant": resource["defaultVariant"],
        "variants": [v[get_query_names[resource_type][3]] for v in variants_list]
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
    features_list = db.getNameVariant("feature_variant", "name", name, "variant", variant)
    feature = features_list[0]

    returned_training_sets_list = get_related_resources("training_set_features", 
                                                        name_col="trainingSetName", 
                                                        var_col="trainingSetVariant", 
                                                        column1="featureName", 
                                                        name=feature["featureName"], 
                                                        column2="featureVariant", 
                                                        variant=feature["variantName"])

    returned_feature = {
        "name": feature["name"],
        "variant": feature["variant"],
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
    labels_list = db.getNameVariant("label_variant", "name", name, "variant", variant)
    label = labels_list[0]

    returned_training_sets_list = get_related_resources("training_set_variant", 
                                                        name_col="trainingSetName", 
                                                        var_col="variantName", 
                                                        column1="labelName", 
                                                        name=label["labelName"], 
                                                        column2="labelVariant", 
                                                        variant=label["variantName"])

    returned_label = {
        "name": label["name"],
        "variant": label["variant"],
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
    source_list = db.getNameVariant("source_variant", "name", name, "variant", variant)#changesource
    source = source_list[0]

    returned_features_list = get_related_resources("feature_variant", 
                                                    name_col="name", 
                                                    var_col="variant", 
                                                    column1="sourceName", 
                                                    name=name, 
                                                    column2="sourceVariant", 
                                                    variant=variant)
    returned_labels_list = get_related_resources("label_variant", 
                                                    name_col="name", 
                                                    var_col="variant", 
                                                    column1="sourceName", 
                                                    name=name, 
                                                    column2="sourceVariant", 
                                                    variant=variant)

    training_sets_list = set()
    for f in returned_features_list:
        training_set_features_query_list = get_related_resources("training_set_features", 
                                                                    column1="featureName", 
                                                                    name="name", 
                                                                    column2="featureVariant", 
                                                                    variant="variant", 
                                                                    format=False)
        for t in training_set_features_query_list:
            training_sets_list.add(t)

    returned_training_sets_list = [{ "name": f["trainingSetName"], "variant": f["trainingSetVariant"]} for f in training_sets_list]
    
    training_sets_list = set()

    for l in returned_labels_list:
        training_set_features_query_list = get_related_resources("training_set_variant", 
                                                                    column1="name", 
                                                                    name="name", 
                                                                    column2="variant", 
                                                                    variant="variant", 
                                                                    format=False)
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

    returned_features_list = get_related_resources("training_set_features", 
                                                    name_col="featureName", 
                                                    var_col="featureVariant", 
                                                    column1="trainingSetName", 
                                                    name=name, 
                                                    column2="trainingSetVariant", 
                                                    variant=variant)

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

def get_provider_info_local(name):
    db = SQLiteMetadata()
    providers_list = db.getVariantResource("providers", "name", name)
    provider = providers_list[0]

    returned_features_list = get_related_resources("feature_variant", 
                                                    name_col="name",
                                                    var_col="variant",
                                                    column1="provider",
                                                    name=name)
                                                    
    returned_labels_list = get_related_resources("label_variant", 
                                                    name_col="name",
                                                    var_col="variant",
                                                    column1="provider",
                                                    name=name)
    returned_provider = {
        "name": provider["name"],
        "type": provider["type"],
        "description": provider["description"],
        "type": provider["providerType"],
        "software": provider["software"],
        "team": provider["team"],
        "status": provider["status"],
        "serializedConfig": provider["serializedConfig"],
        "sources": provider["sources"],
        "features": returned_features_list,
        "labels": returned_labels_list
    }
    format_rows([("NAME: ", returned_provider["name"]),
    ("DESCRIPTION: ", returned_provider["description"]),
    ("TYPE: ", returned_provider["type"]),
    ("SOFTWARE: ", returned_provider["software"]),
    ("TEAM: ", returned_provider["team"]),
    ("STATUS: ", returned_provider["status"])])
    format_rows("SOURCE:", returned_provider["sources"])
    format_new_para("FEATURES:")
    format_rows("NAME", "VARIANT")
    for f in returned_provider["features"]:
        format_rows(f["name"], f["variant"])
    format_new_para("LABELS:")
    format_rows("NAME", "VARIANT")
    for l in returned_provider["labels"]:
        format_rows(l["name"], l["variant"])
    format_new_para("")
    return returned_provider

def get_related_resources(table, column1, name, column2=None, variant=None, name_col="name", var_col="variant", format=True):
    db = SQLiteMetadata()
    if column2 and variant:
        try:
            res_list = db.getNameVariant(table, column1, name, column2, variant)
        except ValueError:
            res_list = []
    else:
        try:
            res_list = db.getVariantResource(table, column1, name)
        except ValueError:
            res_list = []
    if format:
        formatted_res = [{ "name": r[name_col], "variant": r[var_col]} for r in res_list]
        return formatted_res
    return res_list
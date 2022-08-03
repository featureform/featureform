from .format import *
from .sqlite_metadata import *

def get_user_info_local(name):
    user = get_resource("user", name)

    format_rows("USER NAME: ", user["name"])
    format_rows("TYPE: ", user["type"])
    format_rows("STATUS: ", user["status"])
    return user

def get_entity_info_local(name):
    db = SQLiteMetadata()
    entity = get_resource("entity", name)
    
    returned_features_query_list = get_related_resources("feature_variant", "entity", name)
    returned_features_list = format_resource_list(returned_features_query_list)

    returned_labels_query_list = get_related_resources("label_variant", "entity", name)
    returned_labels_list = format_resource_list(returned_labels_query_list)

    training_set_list = set()
    for f in returned_features_list:
        try:
            training_set_features_query_list = db.get_training_set_from_features(f["name"], f["variant"])
        except ValueError:
            training_set_features_query_list = []
        for t in training_set_features_query_list:
            training_set_list.add(t)

    returned_training_sets_list = format_resource_list(training_set_list, "training_set_name", "training_set_variant")
    
    returned_entity = {
        "name": name,
        "status": entity["status"],
        "features": returned_features_list,
        "labels": returned_labels_list,
        "trainingsets": returned_training_sets_list
    }

    format_rows([("ENTITY NAME: ", returned_entity["name"]),
    ("STATUS: ", returned_entity["status"])])
    format_pg()
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
    format_pg()
    return returned_entity

def get_resource_info_local(resource_type, name):
    resource = get_resource(resource_type, name)

    variants_list = get_variant_list(resource_type, name)

    returned_resource_list = {
        "name": resource["name"],
        "default_variant": resource["default_variant"],
        "variants": [v["variant"] for v in variants_list]
    }
   
    format_rows("NAME: ", returned_resource_list["name"])
    if "status" in resource:
        format_rows("STATUS: ", resource["status"])
    format_pg("VARIANTS:")
    format_rows(returned_resource_list["default_variant"], 'default')
    for v in returned_resource_list["variants"]:
        if v != returned_resource_list["default_variant"]:
            format_rows(v, '')
    format_pg()
    return returned_resource_list

def get_feature_variant_info_local(name, variant):
    db = SQLiteMetadata()
    feature = get_variant("feature", name, variant)

    try:
        returned_training_sets_query_list = db.get_training_set_from_features(feature["name"], feature["variant"])
    except ValueError:
        returned_training_sets_query_list = []
    returned_training_sets_list = format_resource_list(returned_training_sets_query_list, "training_set_name", "training_set_variant")

    returned_feature = {
        "name": feature["name"],
        "variant": feature["variant"],
        "type": feature["data_type"],
        "entity": feature["entity"],
        "owner": feature["owner"],
        "description": feature["description"],
        "provider": feature["provider"],
        "status": feature["status"],
        "source": { 
            "name": feature["source_name"],
            "variant": feature["source_variant"]
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
    format_pg("SOURCE: ")
    format_rows([("NAME", "VARIANT"), (returned_feature["source"]["name"], returned_feature["source"]["variant"])])
    format_pg("TRAINING SETS:")
    format_rows("NAME", "VARIANT")
    for t in returned_feature["trainingsets"]:
        format_rows(t["name"], t["variant"])
    format_pg()
    return returned_feature

def get_label_variant_info_local(name, variant):
    db = SQLiteMetadata()
    label = get_variant("label", name, variant)

    try:
        returned_training_sets_query_list = db.get_training_set_from_labels(label["name"], label["variant"])
    except ValueError:
        returned_training_sets_query_list = []
    
    returned_training_sets_list = format_resource_list(returned_training_sets_query_list)

    returned_label = {
        "name": label["name"],
        "variant": label["variant"],
        "type": label["data_type"],
        "entity": label["entity"],
        "owner": label["owner"],
        "description": label["description"],
        "provider": label["provider"],
        "status": label["status"],
        "source": { 
            "name": label["source_name"],
            "variant": label["source_variant"]
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
    format_pg("SOURCE: ")
    format_rows([("NAME", "VARIANT"), (returned_label["source"]["name"], returned_label["source"]["variant"])])
    format_pg("TRAINING SETS:")
    format_rows("NAME", "VARIANT")
    for t in returned_label["trainingsets"]:
        format_rows(t["name"], t["variant"])
    format_pg()
    return returned_label

def get_source_variant_info_local(name, variant):
    db = SQLiteMetadata()
    source = get_variant("source", name, variant)

    try:
        returned_features_query_list = db.get_feature_variants_from_source(name, variant)
        returned_features_list = format_resource_list(returned_features_query_list)
    except ValueError:
        returned_features_list = []

    try:
        returned_labels_query_list = db.get_label_variants_from_source(name, variant)
        returned_labels_list = format_resource_list(returned_labels_query_list)
    except ValueError:
        returned_labels_list = []

    training_set_list = set()

    for f in returned_features_list:
        try:
            training_set_features_query_list = db.get_training_set_from_features(f["name"], f["variant"])
        except ValueError:
            training_set_features_query_list = []
        for t in training_set_features_query_list:
            training_set_list.add(t)
    
    for l in returned_labels_list:
        try:
            training_set_labels_query_list = db.get_training_set_variant_from_label(l["name"], l["variant"])
        except ValueError:
            training_set_labels_query_list = []
        for t in training_set_labels_query_list:
            training_set_list.add(t)

    returned_training_sets_list = format_resource_list(training_set_list)
    
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
    format_pg("DEFINITION:")
    print(returned_source["definition"])
    print("FEATURES:")
    format_rows("NAME", "VARIANT")
    for f in returned_source["features"]:
        format_rows(f["name"], f["variant"])
    format_pg("LABELS:")
    format_rows("NAME", "VARIANT")
    for l in returned_source["labels"]:
        format_rows(l["name"], l["variant"])
    format_pg("TRAINING SETS:")
    format_rows("NAME", "VARIANT")
    for t in returned_source["trainingsets"]:
        format_rows(t["name"], t["variant"])
    format_pg()
    return returned_source

def get_training_set_variant_info_local(name, variant):
    db = SQLiteMetadata()
    training_set = get_variant("training-set", name, variant)

    try:
        returned_features_query_list = db.get_training_set_features(name, variant)
    except ValueError:
        returned_features_query_list = []
    returned_features_list = format_resource_list(returned_features_query_list, "feature_name", "feature_variant")
    
    returned_training_set = {
        "name": training_set["name"],
        "variant": training_set["variant"],
        "owner": training_set["owner"],
        "description": training_set["description"],
        "status": training_set["status"],
        "label": {
            "name": training_set["label_name"],
            "variant": training_set["label_variant"]
        },
        "features": returned_features_list
    }

    format_rows([("NAME: ", returned_training_set["name"]),
    ("VARIANT: ", returned_training_set["variant"]),
    ("OWNER:", returned_training_set["owner"]),
    ("DESCRIPTION:", returned_training_set["description"]),
    ("STATUS: ", returned_training_set["status"])])
    format_pg("LABEL: ")
    format_rows([("NAME", "VARIANT"), (returned_training_set["label"]["name"], returned_training_set["label"]["variant"])])
    format_pg("FEATURES:")
    format_rows("NAME", "VARIANT")
    for f in returned_training_set["features"]:
        format_rows(f["name"], f["variant"])
    format_pg()
    return returned_training_set

def get_provider_info_local(name):
    db = SQLiteMetadata()
    provider = get_resource("provider", name)

    try:
        returned_features_query_list = db.get_feature_variants_from_provider(name)
        returned_features_list = format_resource_list(returned_features_query_list)
    except ValueError:
        returned_features_list = []

    try:
        returned_labels_query_list = db.get_label_variants_from_provider(name)
        returned_labels_list = format_resource_list(returned_labels_query_list)                         
    except ValueError:
        returned_labels_list = []
    
    returned_provider = {
        "name": provider["name"],
        "type": provider["type"],
        "description": provider["description"],
        "type": provider["provider_type"],
        "software": provider["software"],
        "team": provider["team"],
        "status": provider["status"],
        "serializedConfig": provider["serialized_config"],
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
    format_pg("FEATURES:")
    format_rows("NAME", "VARIANT")
    for f in returned_provider["features"]:
        format_rows(f["name"], f["variant"])
    format_pg("LABELS:")
    format_rows("NAME", "VARIANT")
    for l in returned_provider["labels"]:
        format_rows(l["name"], l["variant"])
    format_pg()
    return returned_provider

def get_related_resources(table, column, name):
    db = SQLiteMetadata()
    try:
        res_list = db.query_resource(table, column, name)
    except ValueError:
        res_list = []
    return res_list

def get_resource(resource_type, name):
    db = SQLiteMetadata()
    resource_sql_functions = {
        "user": db.get_user,
        "entity": db.get_entity,
        "feature": db.get_feature,
        "label": db.get_label,
        "source": db.get_source,
        "training-set": db.get_training_set,
        "model": db.get_model,
        "provider": db.get_provider
    }
    res_list = resource_sql_functions[resource_type](name)
    return res_list

def get_variant_list(resource_type, name):
    db = SQLiteMetadata()
    variant_list_sql_functions = {
        "feature": db.get_feature_variants_from_feature,
        "label": db.get_label_variants_from_label,
        "source": db.get_source_variants_from_source,
        "training-set": db.get_training_set_variant_from_training_set
    }
    
    res_list = variant_list_sql_functions[resource_type](name)
    return res_list

def get_variant(resource_type, name, variant):
    db = SQLiteMetadata()

    variant_sql_functions = {
        "feature": db.get_feature_variant,
        "label": db.get_label_variant,
        "source": db.get_source_variant,
        "training-set": db.get_training_set_variant
    }

    res_list = variant_sql_functions[resource_type](name, variant)
    return res_list

def format_resource_list(res_list, name_col="name", var_col="variant"):
    formatted_res = [{ "name": r[name_col], "variant": r[var_col]} for r in res_list]
    return formatted_res

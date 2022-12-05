from .format import *
from .sqlite_metadata import *

def get_user_info_local(name):
    return User(
        name = name,
    )

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
    return Feature(
        name=returned_feature["name"],
        variant=returned_feature["variant"],
        value_type=returned_feature["type"],
        entity=returned_feature["entity"],
        owner=returned_feature["owner"],
        description=returned_feature["description"],
        provider=returned_feature["provider"],
        status=returned_feature["status"],
        source=(returned_feature["source"]["name"], returned_feature["source"]["variant"]),
        trainingsets=[(f["name"], f["variant"]) for f in returned_feature["trainingsets"]],
    )

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
    return Label(
        name=returned_label["name"],
        variant=returned_label["variant"],
        value_type=returned_label["value_type"],
        entity=returned_label["entity"],
        owner=returned_label["owner"],
        description=returned_label["description"],
        provider=returned_label["provider"],
        status=returned_label["status"],
        source=(returned_label["source"]["name"], returned_label["source"]["variant"]),
        trainingsets=[(f["name"], f["variant"]) for f in returned_label["trainingsets"]],
    )

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

    return Source(
        name=returned_source["name"],
        variant=returned_source["variant"],
        owner=returned_source["owner"],
        description=returned_source["description"],
        provider=returned_source["provider"],
        status=returned_source["status"],
        # definition,
        features=[(f["name"], f["variant"]) for f in returned_source["features"]],
        labels=[(f["name"], f["variant"]) for f in returned_source["labels"]],
        trainingsets=[(f["name"], f["variant"]) for f in returned_source["trainingsets"]],
    )

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

    return TrainingSet(
        name=returned_training_set["name"],
        variant=returned_training_set["variant"],
        owner=returned_training_set["owner"],
        description=returned_training_set["description"],
        status=returned_training_set["status"],
        label=(returned_training_set["label"]["name"], returned_training_set["label"]["variant"]),
        features=[(f["name"], f["variant"]) for f in returned_training_set["features"]],
    )

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

    return Provider(
        name=returned_provider["name"],
        description=returned_provider["description"],
        provider_type=returned_provider["type"],
        software=returned_provider["software"],
        team=returned_provider["team"],
        status=returned_provider["status"],
        sources=[(f["name"], f["variant"]) for f in returned_provider["sources"]],
        features=[(f["name"], f["variant"]) for f in returned_provider["features"]],
        labels=[(f["name"], f["variant"]) for f in returned_provider["labels"]],
        training_sets=[(f["name"], f["variant"]) for f in returned_provider["trainingsets"]],
    )

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
from .sqlite_metadata import *
from .format import *

cutoff_length = 60

def get_sorted_list(resource_type, variant=False):
    db = SQLiteMetadata()
    local_list_args = {
        "entity": ["entities"],
        "user": ["users"],
        "model": ["models"],
        "provider": ["providers"],
        "feature": ["features", "feature_variant", "featureName"],
        "label": ["labels", "label_variant", "labelName"],
        "source": ["sources", "source_variant", "name"], #changesource
        "training-set": ["training_sets", "training_set_variant", "trainingSetName"]
    }
    if variant:
        res = sorted([received for received in db.getTypeTable(local_list_args[resource_type][1])], key=lambda x:x[local_list_args[resource_type][2]])
    else:
        res = sorted([received for received in db.getTypeTable(local_list_args[resource_type][0])], key=lambda x:x["name"])
    return res

def list_name_status_local(resource_type):
    format_rows("NAME", "STATUS")
    res = get_sorted_list(resource_type)
    for r in res:
        format_rows(r["name"], r["status"])
    return res

def list_name_status_desc_local(resource_type):
    format_rows("NAME", "STATUS", "DESCRIPTION")
    res = get_sorted_list(resource_type)
    for r in res:
        format_rows(r["name"], r["status"], r["description"][:cutoff_length])
    return res

def list_name_variant_status_local(resource_type):
    format_rows("NAME", "VARIANT", "STATUS")
    res = get_sorted_list(resource_type)
    for r in res:
        res_variants = get_sorted_list(resource_type, True)
        for v in res_variants:
            if r["defaultVariant"] == v["variantName"]:
                format_rows(r["name"], f"{r['defaultVariant']} (default)", v["status"])
            else:
                format_rows(r["name"], v["variantName"], v["status"])
    return res

def list_name_variant_status_desc_local(resource_type):
    format_rows("NAME", "VARIANT", "STATUS", "DESCRIPTION")
    res = get_sorted_list(resource_type)
    for r in res:
        res_variants = get_sorted_list(resource_type, True)
        for v in res_variants:
            if r["defaultVariant"] == v["variantName"]:
                format_rows(r["name"], f"{r['defaultVariant']} (default)", v["status"], v["description"])
            else:
                format_rows(r["name"], v["variantName"], v["status"], v["description"])
    return res
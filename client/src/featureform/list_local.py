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
        "feature": ["features", "feature_variant"],
        "label": ["labels", "label_variant"],
        "source": ["sources", "source_variant"],
        "training-set": ["training_sets", "training_set_variant"]
    }
    if variant:
        received = db.getTypeTable(local_list_args[resource_type][1])
        res = sorted([r for r in received], key=lambda x:x["name"])
    else:
        received =  db.getTypeTable(local_list_args[resource_type][0])
        res = sorted([r for r in received], key=lambda x:x["name"])
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
    res_variants = get_sorted_list(resource_type, True)
    for r in res:
        for v in filter(lambda x: x["name"] == r["name"], res_variants):
            if r["default_variant"] == v["variant"]:
                format_rows(r["name"], f"{r['default_variant']} (default)", v["status"])
            else:
                format_rows(r["name"], v["variant"], v["status"])
    return res

def list_name_variant_status_desc_local(resource_type):
    print(resource_type)
    format_rows("NAME", "VARIANT", "STATUS", "DESCRIPTION")
    res = get_sorted_list(resource_type)
    res_variants = get_sorted_list(resource_type, True)
    for r in res:
        for v in filter(lambda x: x["name"] == r["name"], res_variants):
            if r["default_variant"] == v["variant"]:
                format_rows(r["name"], f"{r['default_variant']} (default)", v["status"], v["description"])
            else:
                format_rows(r["name"], v["variant"], v["status"], v["description"])
    return res
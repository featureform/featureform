from .sqlite_metadata import *
from .format import *
from enum import Enum

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
        received = db.get_type_table(local_list_args[resource_type][1])
        res = sorted([r for r in received], key=lambda x:x["name"])
    else:
        received =  db.get_type_table(local_list_args[resource_type][0])
        res = sorted([r for r in received], key=lambda x:x["name"])
    return res

class ColumnName(Enum):
    NAME = 0
    STATUS = 1
    VARIANT = 2
    DESCRIPTION = 3

def list_conditions(x, clm_name, default_variant=None):
    if clm_name == "variant" and default_variant == x[clm_name]:
        return f"{x[clm_name]} (default)"
    if clm_name == "description":
        return x[clm_name][:cutoff_length]
    return x[clm_name]

def list_local(resource_type, fields):
    field_to_clm = {
        ColumnName.NAME: "name",
        ColumnName.VARIANT: "variant",
        ColumnName.STATUS: "status",
        ColumnName.DESCRIPTION: "description"
    }
    clm_names = [field_to_clm[field] for field in fields]
    format_rows(*[field.name for field in fields])
    res = get_sorted_list(resource_type)
    if "variant" in clm_names:
        res_variants = get_sorted_list(resource_type, True)
        for r in res:
            for v in filter(lambda x: x["name"] == r["name"], res_variants):
                format_list = [list_conditions(v, clm_name, r["default_variant"]) for clm_name in clm_names]
                format_rows(*format_list)
    else:
        for r in res:
            format_list = [list_conditions(r, clm_name) for clm_name in clm_names]
            format_rows(*format_list)
    return res

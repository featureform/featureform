import pandas as pd
import os
from .register import ScalarType


def get_provider(provider):
    if provider == "file":
        return LocalFile()
    else:
        raise NotImplementedError()


class OnlineStore:
    def create_table(self, name, variant, entity_type):
        raise NotImplementedError()

    def get_table(self, name, variant):
        raise NotImplementedError()

    def is_vector_store(self):
        raise NotImplementedError()


class OnlineTable:
    def set(self, key, value):
        raise NotImplementedError()

    def get(self, key):
        raise NotImplementedError()


class OnlineVectorTable:
    def nearest(self, vector):
        raise NotImplementedError()


class LocalFileTable(OnlineTable):
    def __init__(self, name, variant, root_path, stype):
        self.name = name
        self.variant = variant
        self.root_path = root_path
        self.dir = f"{root_path}/{name}/"
        self.filepath = f"{root_path}/{name}/{variant}.csv"
        self.stype = stype
        os.makedirs(self.dir, exist_ok=True)

    def get_path(self):
        return self.filepath

    def set(self, key, value):
        df = pd.DataFrame(data={"entity": [key], "value": [value]}).astype(
            {"entity": "str", "value": self.stype}
        )
        df.to_csv(self.filepath, mode="a", index=False, header=False)

    def get(self, key):
        df = pd.read_csv(self.filepath)
        value = df[df["entity"] == key]["value"].astype(self.stype).values[0]
        if self.stype == "datetime64[ns]":
            return pd.to_datetime(value)
        return value


class LocalFile(OnlineStore):
    def __init__(self):
        self.path = ".featureform/features"
        self.type_table = f"{self.path}/types.csv"
        os.makedirs(self.path, exist_ok=True)

    def create_table(self, name, variant, entity_type: ScalarType):
        if os.path.exists(f"{self.path}/{name}/{variant}.csv"):
            raise ValueError(f"Table {name} with variant {variant} already exists")
        types_df = pd.DataFrame(
            data={"name": [name], "variant": [variant], "type": [entity_type.value]}
        ).astype({"name": "string", "variant": "string", "type": "string"})
        header = True
        if os.path.exists(self.type_table):
            header = False
        types_df.to_csv(self.type_table, mode="a", header=header, index=False)
        table = LocalFileTable(name, variant, self.path, self.__get_type(entity_type))
        df = pd.DataFrame(data={"entity": [], "value": []}).astype(
            {"entity": "string", "value": self.__get_type(entity_type)}
        )
        df.to_csv(table.get_path(), index=False)
        return table

    def get_table(self, name, variant):
        if not os.path.exists(f"{self.path}/{name}/{variant}.csv"):
            raise ValueError(f"Table {name} with variant {variant} does not exist")
        df = pd.read_csv(self.type_table)
        matched_rows = df.loc[
            (df["name"] == str(name)) & (df["variant"] == str(variant))
        ]
        if len(matched_rows) == 0:
            raise ValueError(
                f"Could not get type for table {name} with variant {variant}"
            )
        stype = ScalarType(matched_rows["type"].values[0])
        return LocalFileTable(self.path, name, variant, stype)

    def is_vector_store(self):
        return False

    def __get_type(self, stype: ScalarType):
        types = {
            ScalarType.NIL: "",
            ScalarType.INT: "int64",
            ScalarType.INT32: "int64",
            ScalarType.INT64: "int64",
            ScalarType.FLOAT32: "float64",
            ScalarType.FLOAT64: "float64",
            ScalarType.STRING: "object",
            ScalarType.BOOL: "bool",
            ScalarType.DATETIME: "datetime64[ns]",
        }
        return types[stype]

import pandas as pd
import os
from ..register import ScalarType
from .online_store import OnlineStore, OnlineStoreTable, ValueType


class LocalFileTable(OnlineStoreTable):
    def __init__(self, name, variant, root_path, stype):
        self.name = name
        self.variant = variant
        self.root_path = root_path
        self.dir = f"{root_path}/{name}/"
        self.filepath = f"{root_path}/{name}/{variant}.csv"
        self.stype = stype
        self.update_df = self.__init_update_df()

        if not os.path.exists(self.dir):
            os.makedirs(self.dir, exist_ok=True)

        if not os.path.exists(self.filepath):
            self.__init_table()
        else:
            self.df = pd.read_csv(self.filepath, header=0, names=["entity", "value"])

    def __init_table(self):
        self.df = pd.DataFrame(data={"entity": [], "value": []}).astype(
            {"entity": "string", "value": self.stype}
        )
        self.df.to_csv(self.filepath, index=False)

    def __init_update_df(self):
        df = pd.DataFrame(columns=["entity", "value"]).astype(
            {"entity": "str", "value": self.stype}
        )
        return df

    def set(self, key, value):
        df = pd.DataFrame(data={"entity": [key], "value": [value]})
        try:
            df = df.astype({"entity": "str", "value": self.stype})
        except:
            raise ValueError(f"Could not cast feature to type {self.stype}")
        self.df = self.df.append(df)

    def set_batch(self, df):
        columns = df.columns
        df = (
            df.reset_index(drop=True)
            .rename(columns={columns[0]: "entity", columns[1]: "value"})
            .drop_duplicates("entity", keep="last")
            .sort_values("entity", ascending=False)
        )
        df.to_csv(self.filepath, mode="w", index=False, header=True)

    def get(self, key):
        # The most efficient way to perform the upsert at the moment is to
        # create an array with new values to be inserted then process them
        # all at the same time in memory.
        self.flush()
        df = self.df
        try:
            value = df[df["entity"] == key]["value"].astype(self.stype).values[0]
        except IndexError:
            raise ValueError(
                f"Could not find value for feature: {self.name} ({self.variant}) for: {key}"
            ) from None
        if self.stype == "datetime64[ns]":
            return pd.to_datetime(value)
        return value

    def flush(self):
        self.df = (
            pd.concat([self.df, self.update_df])
            .drop_duplicates("entity", keep="last")
            .sort_values("entity", ascending=False)
            .reset_index(drop=True)
        )
        self.df.to_csv(self.filepath, mode="w", index=False, header=True)


class LocalFileStore(OnlineStore):
    def __init__(self, config):
        self.path = ".featureform/features"
        self.type_table = f"{self.path}/types.csv"
        if not os.path.exists(self.path):
            os.makedirs(self.path, exist_ok=True)

    def create_table(self, name, variant, entity_type: ValueType):
        if os.path.exists(f"{self.path}/{name}/{variant}.csv"):
            raise ValueError(f"Table {name} with variant {variant} already exists")

        self.__set_type(name, variant, entity_type)

        table = LocalFileTable(
            name, variant, self.path, self.__convert_type(entity_type.scalar())
        )
        return table

    def __set_type(self, name, variant, entity_type: ValueType):
        header = True
        if os.path.exists(self.type_table):
            header = False

        types_df = pd.DataFrame(
            data={
                "name": [name],
                "variant": [variant],
                "type": [entity_type.scalar().value],
            }
        ).astype({"name": "string", "variant": "string", "type": "string"})

        types_df.to_csv(self.type_table, mode="a", header=header, index=False)

    def get_table(self, name, variant):
        if not os.path.exists(f"{self.path}/{name}/{variant}.csv"):
            raise ValueError(f"Table {name} with variant {variant} does not exist")

        stype = self.__get_type(name, variant)

        return LocalFileTable(name, variant, self.path, self.__convert_type(stype))

    def __get_type(self, name, variant) -> ScalarType:
        df = pd.read_csv(self.type_table)
        matched_rows = df.loc[
            (df["name"] == str(name)) & (df["variant"] == str(variant))
        ]
        if len(matched_rows) == 0:
            raise ValueError(
                f"Could not get type for table {name} with variant {variant}"
            )

        return ScalarType(matched_rows["type"].values[0])

    def table_exists(self, name, variant):
        return os.path.exists(f"{self.path}/{name}/{variant}.csv")

    def __convert_type(self, stype: ScalarType):
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

    def close(self):
        pass

    def delete_table(self, feature: str, variant: str):
        pass

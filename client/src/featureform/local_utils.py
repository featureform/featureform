import re

import pandas as pd


def get_sql_transformation_sources(query_string):
    # Use regex to parse query inputs in double curly braces {{ }}
    return [ts[2:-2].split(".") for ts in re.findall("(?={{).*?(?<=}})", query_string)]


def merge_feature_into_ts(feature_row, label_row, df, trainingset_df):
    if feature_row["source_timestamp"] != "":
        trainingset_df = pd.merge_asof(
            trainingset_df,
            df.sort_values(feature_row["source_timestamp"]),
            direction="backward",
            left_on=label_row["source_timestamp"],
            right_on=feature_row["source_timestamp"],
            left_by=label_row["source_entity"],
            right_by=feature_row["source_entity"],
        )
        if feature_row["source_timestamp"] != label_row["source_timestamp"]:
            trainingset_df.drop(columns=feature_row["source_timestamp"], inplace=True)
    else:
        df.drop_duplicates(
            subset=[feature_row["source_entity"]], keep="last", inplace=True
        )
        trainingset_df.reset_index(inplace=True)
        trainingset_df[label_row["source_entity"]] = trainingset_df[
            label_row["source_entity"]
        ].astype("string")
        df[label_row["source_entity"]] = df[label_row["source_entity"]].astype("string")
        trainingset_df = trainingset_df.join(
            df.set_index(label_row["source_entity"]),
            how="left",
            on=label_row["source_entity"],
            lsuffix="_left",
        )
        if "index" in trainingset_df.columns:
            trainingset_df.drop(columns="index", inplace=True)
    return trainingset_df


def list_to_combined_df(features_list, entity_id):
    all_feature_df = None
    try:
        for feature in features_list:
            if all_feature_df is None:
                all_feature_df = feature
            else:
                all_feature_df = all_feature_df.join(
                    feature.set_index(entity_id), on=entity_id, lsuffix="_left"
                )
        return all_feature_df
    except TypeError:
        print("Set is empty")


def get_features_for_entity(entity_id, entity_value, all_feature_df):
    entity = all_feature_df.loc[all_feature_df[entity_id] == entity_value].copy()
    entity.drop(columns=entity_id, inplace=True)
    if len(entity.values) > 0:
        return entity.values[0]
    else:
        raise Exception(f"No matching entities for {entity_id}: {entity_value}")


def feature_df_with_entity(source_path, entity_id, feature):
    name_variant = f"{feature['name']}.{feature['variant']}"
    df = pd.read_csv(str(source_path))
    check_missing_values(feature, df)
    if feature["source_timestamp"] != "":
        df = df[
            [
                feature["source_entity"],
                feature["source_value"],
                feature["source_timestamp"],
            ]
        ]
        df = df.sort_values(by=feature["source_timestamp"], ascending=True)
        df = df.drop(columns=feature["source_timestamp"])
    else:
        df = df[[feature["source_entity"], feature["source_value"]]]
    df.set_index(feature["source_entity"])
    df.rename(
        columns={
            feature["source_entity"]: entity_id,
            feature["source_value"]: name_variant,
        },
        inplace=True,
    )
    df.drop_duplicates(subset=[entity_id], keep="last", inplace=True)
    return df


def check_missing_values(resource, df):
    if resource["source_entity"] not in df.columns:
        raise KeyError(f"Entity column does not exist: {resource['source_entity']}")
    if resource["source_value"] not in df.columns:
        raise KeyError(f"Value column does not exist: {resource['source_value']}")
    if (
        resource["source_timestamp"] not in df.columns
        and resource["source_timestamp"] != ""
    ):
        raise KeyError(
            f"Timestamp column does not exist: {resource['source_timestamp']}"
        )


def label_df_from_csv(label, file_name):
    df = pd.read_csv(file_name)
    check_missing_values(label, df)
    if label["source_timestamp"] != "":
        df = df[
            [label["source_entity"], label["source_value"], label["source_timestamp"]]
        ]
        df[label["source_timestamp"]] = pd.to_datetime(df[label["source_timestamp"]])
        df.sort_values(by=label["source_timestamp"], inplace=True)
        df.drop_duplicates(
            subset=[label["source_entity"], label["source_timestamp"]],
            keep="last",
            inplace=True,
        )
    else:
        df = df[[label["source_entity"], label["source_value"]]]
    df.set_index(label["source_entity"], inplace=True)
    return df


def feature_df_from_csv(feature, filename):
    df = pd.read_csv(str(filename))
    check_missing_values(feature, df)
    if feature["source_timestamp"] != "":
        df = df[
            [
                feature["source_entity"],
                feature["source_value"],
                feature["source_timestamp"],
            ]
        ]
        df[feature["source_timestamp"]] = pd.to_datetime(
            df[feature["source_timestamp"]]
        )
        df = df.sort_values(by=feature["source_timestamp"], ascending=True)
    else:
        df = df[[feature["source_entity"], feature["source_value"]]]
    return df

#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

import offline_store_spark_runner as ff_runner


def main(args):
    spark = ff_runner.init_spark_config(args.spark_config)
    data = [
        ("A", 10, 100, "2023-01-01"),
        ("A", 20, 200, "2023-01-02"),
        ("B", 30, 300, "2023-01-01"),
        ("B", 40, 400, "2023-01-03"),
    ]
    expected = [("A", 20, 200, "2023-01-02"), ("B", 40, 400, "2023-01-03")]
    columns = ["entity", "value1", "value2", "timestamp"]

    df = spark.createDataFrame(data, columns)
    exp_df = spark.createDataFrame(expected, columns)
    op = ff_runner.LatestFeaturesTransform(
        entity_col="entity", value_cols=["value1", "value2"], timestamp_col="timestamp"
    )
    result = op.apply(df)
    result_set = set(result.collect())
    exp_set = set(exp_df.collect())
    if result_set != exp_set:
        raise Exception(
            f"Dataframe not equal\nFound: {result_set}\nExpected: {exp_set}\n"
        )


if __name__ == "__main__":
    main(ff_runner.parse_args())

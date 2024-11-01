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
    columns = ["entity", "value1", "value2", "timestamp"]
    df = spark.createDataFrame(data, schema=columns)

    creds = args.credential
    access = creds.get("dynamo_aws_access_key_id")
    secret = creds.get("dynamo_aws_secret_access_key")
    region = creds.get("dynamo_aws_region")
    dynamo = ff_runner.DynamoDBClient(region, access, secret)
    iceberg = ff_runner.IcebergClient(spark, "ff_catalog", "featureform")
    # Run with timestamp
    table_name = "test__materialize_test_w_timestamp"
    feature_name = "materialize_test"
    op = ff_runner.MaterializeToIcebergAndDynamoV1Operation(
        dynamo, iceberg, feature_name, "with_ts", table_name
    )
    try:
        iceberg.delete_table(table_name)
    except:
        pass
    try:
        dynamo.delete_table(table_name)
    except:
        pass
    try:
        dynamo.create_table(table_name, feature_name)
    except Exception as e:
        print(f"Failed to create dynamo table, but will continue: {e}")
    op.run(df, "entity", "value2", "timestamp")

    table_name = "test__materialize_test"
    feature_name = "materialize_test"
    op = ff_runner.MaterializeToIcebergAndDynamoV1Operation(
        dynamo,
        iceberg,
        feature_name,
        "without_ts",
        table_name,
    )
    try:
        iceberg.delete_table(table_name)
    except:
        pass
    try:
        dynamo.delete_table(table_name)
    except:
        pass
    try:
        dynamo.create_table(table_name, feature_name)
    except Exception as e:
        print(f"Failed to create dynamo table, but will continue: {e}")
    op.run(df, "entity", "value2")


if __name__ == "__main__":
    main(ff_runner.parse_args())

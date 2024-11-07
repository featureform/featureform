#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

import offline_store_spark_runner as ff_runner


def main(args):
    spark = ff_runner.init_spark_config(args.spark_config)
    keys = [str(i) for i in range(1_000)]
    data = [(key, "a", 100 - int(key)) for key in keys]
    columns = ["id", "name", "age"]
    df = spark.createDataFrame(data, schema=columns)
    table_name = "spark_test"

    creds = args.credential
    access = creds.get("dynamo_aws_access_key_id")
    secret = creds.get("dynamo_aws_secret_access_key")
    region = creds.get("dynamo_aws_region")

    client = ff_runner.DynamoDBClient(region, access, secret)
    try:
        client.delete_table(table_name)
    except:
        pass

    if client.table_exists(table_name):
        raise Exception("Table already exists")
    try:
        client.create_table(table_name, "id")
        print("created")
        if not client.table_exists(table_name):
            raise Exception("Created table but not exists")
        print("exists")
        client.write_df(table_name, df)
        table = client.dynamodb.Table(table_name)
        for key in keys:
            response = table.get_item(Key={"id": key}, ConsistentRead=True)
            if "Item" not in response:
                raise Exception(f"{id} not found in dynamo table")
    except Exception as e:
        print(e)
        raise e
    finally:
        client.delete_table(table_name)


if __name__ == "__main__":
    main(ff_runner.parse_args())

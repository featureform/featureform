#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

import offline_store_spark_runner as ff_runner
from pyspark.sql.types import StringType, StructField, StructType


def main(args):
    spark = ff_runner.init_spark_config(args.spark_config)

    # Kafka bootstrap servers and topics
    kafka_bootstrap_servers = (
        "boot-njm0jtzo.c1.kafka-serverless.us-east-1.amazonaws.com:9098"
    )
    output_topic = "test_topic"
    checkpoint_location = "s3://ali-dev-bucket/checkpoint"

    with ff_runner.KafkaClient(
        kafka_bootstrap_servers=kafka_bootstrap_servers,
        kafka_topic=output_topic,
        num_partitions=1,
        replication_factor=1,
    ) as kafka_client:

        # kafka_client.delete()
        # Example: Writing data to Kafka
        data_to_send = [("key1", {"message": "hello"}), ("key2", {"message": "world"})]
        kafka_client.write(data_to_send)

        # Example: Check if the topic exists
        if kafka_client.exists():
            print("Kafka topic exists!")

    # Step 1: Create a PySpark DataFrame manually
    data = [("1", "Hello Kafka"), ("2", "Writing to Kafka"), ("3", "Spark and Kafka")]
    schema = StructType(
        [
            StructField("key", StringType(), True),
            StructField("value", StringType(), True),
        ]
    )

    df = spark.createDataFrame(data, schema)

    # Step 2: Write the DataFrame to Kafka (once)
    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").write.format(
        "kafka"
    ).option("kafka.bootstrap.servers", kafka_bootstrap_servers).option(
        "topic", output_topic
    ).option(
        "kafka.security.protocol", "SASL_SSL"
    ).option(
        "kafka.sasl.mechanism", "AWS_MSK_IAM"
    ).option(
        "kafka.sasl.jaas.config",
        "software.amazon.msk.auth.iam.IAMLoginModule required;",
    ).option(
        "kafka.sasl.client.callback.handler.class",
        "software.amazon.msk.auth.iam.IAMClientCallbackHandler",
    ).save()

    # Step 3: Read the data back from the same Kafka topic (once)
    read_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", output_topic)
        .option("startingOffsets", "earliest")
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "AWS_MSK_IAM")
        .option(
            "kafka.sasl.jaas.config",
            "software.amazon.msk.auth.iam.IAMLoginModule required;",
        )
        .option(
            "kafka.sasl.client.callback.handler.class",
            "software.amazon.msk.auth.iam.IAMClientCallbackHandler",
        )
        .load()
    )

    # Convert the key and value back to strings and display (read once)
    read_df.selectExpr(
        "CAST(key AS STRING)", "CAST(value AS STRING)"
    ).writeStream.outputMode("append").format("console").trigger(
        once=True
    ).start().awaitTermination()


if __name__ == "__main__":
    main(ff_runner.parse_args())

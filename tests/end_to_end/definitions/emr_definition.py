# import os
# from dotenv import load_dotenv
#
# import featureform as ff
#
# FILE_DIRECTORY = os.getenv("FEATUREFORM_TEST_PATH", "")
# featureform_location = os.path.dirname(os.path.dirname(FILE_DIRECTORY))
# env_file_path = os.path.join(featureform_location, ".env")
# load_dotenv(env_file_path)
#
#
# def get_random_string():
#     import random
#     import string
#     return "".join(random.choice(string.ascii_lowercase) for _ in range(10))
#
#
# def save_version(version):
#     global FILE_DIRECTORY
#     with open(f"{FILE_DIRECTORY}/version.txt", "w+") as f:
#         f.write(version)
#
#
# VERSION = get_random_string()
# os.environ["TEST_CASE_VERSION"] = VERSION
# save_version(VERSION)
#
# # Start of Featureform Definitions
# ff.register_user("featureformer").make_default_owner()
#
# # Register blob store because it can be used standalone as an online store as well
# azure_blob = ff.register_S3_store(
#     name="",
#     bucket_path="",
#     bucket_region="",
#     aws_access_key_id="",
#     aws_secret_access_key="",
# )
#
# # Databricks/EMR just has credentials because it is just a means of processing
# emr = ff.EMRCredentials(
#     aws_access_key_id="",
#     aws_secret_access_key="",
#     emr_cluster_id="",
#     emr_cluster_region="",
# )
#
# # Register spark as the pair of a filestore + executor credentials
# spark = ff.register_spark(
#     executor=emr,
#     filestore=azure_blob
# )
#
# redis = ff.register_redis(
#     name=f"redis-quickstart_{VERSION}",
#     host="quickstart-redis",  # The internal dns name for redis
#     port=6379,
#     description="A Redis deployment we created for the Featureform quickstart"
# )
#
# transactions = spark.register_parquet_file(
#     name=f"transactions_{VERSION}",
#     file_path="transaction_short.csv",
# )
#
#
# @spark.df_transformation(name=f"average_user_transaction_{VERSION}",
#                          variant="quickstart",
#                          inputs=[(f"transactions_{VERSION}", "quickstart")])
# def average_user_transaction(df):
#     from pyspark.sql.functions import avg, col
#     df = df.groupBy("CustomerID").agg(avg("TransactionAmount").alias("avg_transaction_amt"))
#     return df.select(col("CustomerID").alias("user_id"), col("avg_transaction_amt"))
#
#
# user = ff.register_entity("user")
#
# # Register a column from our transformation as a feature
# average_user_transaction.register_resources(
#     entity=user,
#     entity_column="user_id",
#     inference_store=redis,
#     features=[
#         {"name": f"avg_transactions_{VERSION}", "variant": "quickstart", "column": "TransactionAmount",
#          "type": "float32"},
#     ],
# )
#
# # Register label from our base Transactions table
# transactions.register_resources(
#     entity=user,
#     entity_column="user_id",
#     labels=[
#         {"name": f"fraudulent_{VERSION}", "variant": "quickstart", "column": "IsFraud", "type": "bool"},
#     ],
# )
#
# ff.register_training_set(
#     f"fraud_training_{VERSION}", "quickstart",
#     label=(f"fraudulent_{VERSION}", "quickstart"),
#     features=[(f"avg_transactions_{VERSION}", "quickstart")],
# )

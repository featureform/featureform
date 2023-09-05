import featureform as ff
import os
from dotenv import load_dotenv

load_dotenv("../../.env")
databricks = ff.DatabricksCredentials(
    host=os.getenv("DATABRICKS_HOST", None),
    token=os.getenv("DATABRICKS_TOKEN", None),
    cluster_id=os.getenv("DATABRICKS_CLUSTER", None),
)

# gcs = ff.register_gcs(
#     name="gcs",
#     credentials=ff.GCPCredentials(
#         project_id=os.getenv("GCP_PROJECT_ID"),
#         credentials_path=os.getenv("GCP_CREDENTIALS_FILE"),
#     ),
#     bucket_name="featureform-test",
#     path="behave",
# )

# s3 = ff.register_s3(
#     name="s3",
#     credentials=ff.AWSCredentials(
#         access_key=os.getenv("AWS_ACCESS_KEY_ID", None),
#         secret_key=os.getenv("AWS_SECRET_ACCESS_KEY", None),
#     ),
#     bucket_name="featureform-spark-testing",
#     bucket_region="us-east-2",
#     path="behave",
# )

azure = ff.register_blob_store(
    name="azure",
    account_name=os.getenv("AZURE_ACCOUNT_NAME", None),
    account_key=os.getenv("AZURE_ACCOUNT_KEY", None),
    container_name=os.getenv("AZURE_CONTAINER_NAME", None),
    path="end_to_end_tests/behave",
)

name = f"databricks-azure"
spark = ff.register_spark(
    name=name,
    description="A Spark deployment we created for the Featureform quickstart",
    team="featureform-team",
    executor=databricks,
    filestore=azure,
)

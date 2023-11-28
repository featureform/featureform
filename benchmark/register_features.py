import os

import featureform as ff

import dotenv

dotenv.load_dotenv()


# postgres = ff.register_postgres(
#     name="postgres",
#     host="172.17.0.1",
#     user="postgres",
#     database="postgres",
#     password="password",
# )

sf = ff.register_snowflake(
    name="snowflake2",
    username=os.getenv("SNOWFLAKE_USERNAME"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    organization=os.getenv("SNOWFLAKE_ORG"),
    database="benchmark",
)


table = sf.register_table(name="generated_data", table="generated", variant="v8")

entity = ff.register_entity("entity")

dynamo = ff.register_dynamodb(
    name="dynamodb",
    region="us-east-1",
    access_key=os.getenv("AWS_ACCESS_KEY"),
    secret_key=os.getenv("AWS_SECRET_KEY"),
)


# redis = ff.register_redis(
#     name="redis-quickstart",
#     host="quickstart-redis",  # The internal dns name for redis
#     port=6379,
#     description="A Redis deployment we created for the Featureform quickstart",
# )
features = []
for i in range(100, 251):
    features.append(
        {
            "name": f"feature_{i}",
            "column": f"feature_{i}",
            "type": "int64",
            "variant": "v10",
        }
    )

table.register_resources(
    entity=entity,
    entity_column="entity",
    inference_store=dynamo,
    features=features,
    timestamp_column="event_timestamp",
)

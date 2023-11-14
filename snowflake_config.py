import random
import featureform as ff
from featureform import ServingClient

num = random.randint(0, 1000000)

# Offline store
snowflake = ff.register_snowflake(
    name = f"snowflake_docs_{num}",
    description = "Offline store",
    team = "SOME_TEAM",
    username = "SOME_USERNAME",
    password = "SOME_PASSWORD",
    account = "SOME_ACCOUNT",
    organization = "SOME_ORGANIZATION",
    database = "SOME_DATABASE",
    schema = "SOME_SCHEMA",
)

# Online store
redis = ff.register_redis(
    name = "redis-quickstart",
    host="host.docker.internal", # The docker dns name for redis
    port=6379,
)

boolean_table = snowflake.register_table(
    name="boolean_table",
    table= "featureform_resource_feature__8a49dead-41a6-48c3-9ee7-7ed75d783fe4__0efdd949-f967-4796-8ed3-6c6f736344e8",
)

number_table = snowflake.register_table(
    name="number_table",
    table="featureform_resource_feature__f4d42278-0889-44d7-9928-8aef22d23c16__6a6e8ff4-8a8f-4217-8096-bb360ae1e99b",
)
string_table = snowflake.register_table(
    name="number_table",
    table="featureform_resource_feature__h656j34d-0889-44d7-9928-8aef22d23c43__6a6e8ff4-8a8f-4217-1045-bb360ae1e99b",
)

@ff.entity
class User:
    boolean_feature = ff.Feature(
        boolean_table[['entity',' value', 'ts']],
        variant="batch_serving_test_15",
        type=ff.Float32,
        inference_store=redis,
    )
    numerical_feature = ff.Feature(
        number_table[['entity',' value', 'ts']],
        variant="batch_serving_test_15",
        type=ff.Float32,
        inference_store=redis,
    )
    string_feature = ff.Feature(
        number_table[['entity',' value', 'ts']],
        variant="batch_serving_test_15",
        type=ff.String,
        inference_store=redis,
    )

serving = ServingClient(host="localhost:7878", insecure=True)

# Serve batch features
batch_features = serving.iterate_feature_set(User.boolean_feature, ("numerical_feature", "batch_serving_test_15"), (User.string_feature.name, User.string_feature.variant))

for i, batch in enumerate(batch_features):
    print(batch)
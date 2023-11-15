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

table1 = snowflake.register_table(
    name="table1",
    table= "featureform_resource_feature__1926ce54-6d29-4094-a291-6f6516d84eed__b63c0ba7-23d8-437d-bbc9-bb0f2c821f0c",
)

table2 = snowflake.register_table(
    name="table2",
    table= "featureform_resource_feature__c1e195e0-961a-40b6-8437-bf823fa14b02__64203c48-1d9b-43e5-a7eb-2a1d5a0a0fc1",
)

table3 = snowflake.register_table(
    name="table3",
    table="featureform_resource_feature__08b1cc23-18ce-4ae7-9ee0-d68216f19079__2e2a8e99-7a60-4e10-98e2-1d17e44ba476",
)
table4 = snowflake.register_table(
    name="table4",
    table="featureform_resource_feature__22833f30-f38a-4d3c-b3a3-d5896711aa33__af02225e-b5ab-4d85-aa76-de03a5839662",
)

@ff.entity
class User:
    table1_feature = ff.Feature(
        table1[['entity',' value', 'ts']],
        variant=f"variant_{num}",
        type=ff.Float32,
        inference_store=redis,
    )
    table2_feature = ff.Feature(
        table2[['entity',' value', 'ts']],
        variant=f"variant_{num}",
        type=ff.Float32,
        inference_store=redis,
    )
    table3_feature = ff.Feature(
        table3[['entity',' value', 'ts']],
        variant=f"variant_{num}",
        type=ff.String,
        inference_store=redis,
    )
    table4_feature = ff.Feature(
        table4[['entity',' value', 'ts']],
        variant=f"variant_{num}",
        type=ff.String,
        inference_store=redis,
    )
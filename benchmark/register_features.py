import featureform as ff

bg = ff.register_bigquery(
    name="bigquery",
    project_id="testing-352123",
    dataset_id="benchmark",
    credentials_path="/Users/sdreyer/Downloads/testing-352123-4f22e8ec73ad.json",
)

table = bg.register_table(
    name="generated_data",
    table="generated",
)

entity = ff.register_entity("entity")

redis = ff.register_redis(
    name="redis-quickstart",
    host="quickstart-redis",  # The internal dns name for redis
    port=6379,
    description="A Redis deployment we created for the Featureform quickstart",
)
features = []
for i in range(250):
    features.append({"name": f"feature_{i}", "column": f"feature_{i}", "type": "int64"})

table.register_resources(
    entity=entity,
    entity_column="entity",
    inference_store=redis,
    features=features,
    timestamp_column="event_timestamp",
)

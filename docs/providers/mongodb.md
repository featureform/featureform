# MongoDB

Featureform supports [MongoDB](https://www.mongodb.com/) as an Inference Store.

## Implementation

A MongoDB collection is created for every feature. 
Each collection contains documents with the materialized values for each feature, making them readily accessible at serving time. 

## Configuration

First we have to add a declarative MongoDB configuration in Python. 
The required fields include a name for the provider, and connection information. The specified database will be created
during the first feature materialization. 

Optionally, maximum throughput can be set when using Cosmos DB for MongoDB with autoscaling.


{% code title="mongodb_config.py" %}
```python
import featureform as ff
mongo = ff.register_mongodb(
    name="mongodb",
    host="my.mongo.host.com",
    port="1000",
    username="my_mongo_user",
    password="secretpassword",
    database="my_database",
    throughput=1000
)
```
{% endcode %}

Once our config file is complete, we can apply it to our Featureform deployment

```bash
featureform apply mongodb_config.py --host $FEATUREFORM_HOST
```

We can re-verify that the provider is created by checking the [Providers tab of the Feature Registry](../getting-started/exploring-the-feature-registry.md).

# Registering Infrastructure Providers

Featureform coordinates a set of infrastructure providers to act together as a feature store. This "Virtual Feature Store" approach allows teams to choose the right infrastructure to meet their needs, and interface across them with the same abstraction. Teams can also use multiple infrastructure providers for different use cases across the organization, while maintaining one unified abstraction across all of them.

## Type of Providers

There are currently two types of providers: Offline Store and Inference Stores. The offline store is where primary data is stored and transformed into features, labels, and training sets. The training sets are served directly from the offline store, while features are materialized into an inference store for serving.

### Inference Store

An inference store allows feature values to be looked up at inference time. Featureform maintains the current value of each feature per entity in the inference store, and provides a [Python API for serving](serving-for-inference-and-training.md).

#### Choosing an Inference Store

When choosing an inference store provider, the three variables to consider are price, deployment complexity, and latency. Deployment complexity refers to the cost of expertise needed to host the inference store provider. That can be the cost of the internal IT headcount, a vendor, or a cloud platform. Price refers to the actual cost of data and serving in the inference store, typically the lower latency an inference store is the higher the cost per GB of storage. In near-real time situations like a recommender system, a low-latency inference store provider like [Redis](../providers/redis.md) or [Cassandra](../providers/cassandra.md) is the right choice. On the other hand, in a batch use case, using [Snowflake](../providers/snowflake.md) may be sufficient and cost-efficient.

### Offline Store

The Offline Store provides dataset storage, transformation capabilities, and training set serving. Featureform coordinates transformations on the offline store to have it reach the user's desired state.

#### Choosing an Offline Store

The Offline Store performs the majority of the heavy lifting for the feature store. It stores the primary sources and runs all of the transformations. Featureform also uses it to create training sets and generate the data for the interference store. The Offline Store provider you chose should be is able to handle your scale of data and support the transformation language you'd like to use: whether it be SQL, Dataframes, or something else.

## Example Config

In this example, we'll configure Featureform with [Postgres](../providers/postgres.md) as our Offline Store and [Redis](../providers/redis.md) as our Inference Store.

We'll begin by specifying our providers in a Python file

{% code title="providers.py" %}
```python
import featureform as ff

redis = ff.register_redis(
    name = "redis",
    description = "Example inference store",
    team = "Featureform",
    host = "0.0.0.0",
    port = 6379,
    password = "",
    db = 0,
)

postgres = ff.register_postgres(
    name = "postgres_docs",
    description = "Example offline store store",
    team = "Featureform",
    host = "0.0.0.0",
    port = 5432,
    user = "postgres",
    password = "password",
    database = "postgres",
)
```
{% endcode %}

Then, we'll use the Featureform CLI to register them

```bash
featureform apply providers.py
```

## Supported Providers

### Inference Stores

{% content-ref url="../providers/redis.md" %}
[redis.md](../providers/redis.md)
{% endcontent-ref %}

{% content-ref url="../providers/cassandra.md" %}
[cassandra.md](../providers/cassandra.md)
{% endcontent-ref %}

### Offline Stores

{% content-ref url="../providers/snowflake.md" %}
[snowflake.md](../providers/snowflake.md)
{% endcontent-ref %}

{% content-ref url="../providers/postgres.md" %}
[postgres.md](../providers/postgres.md)
{% endcontent-ref %}

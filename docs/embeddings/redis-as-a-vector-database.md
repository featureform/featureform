# Redis as a Vector Database

A vector database is any database that supports approximate nearest neighbor (ANN) lookups. Redis natively supports ANN and can be used as a vector database in Featureform.

## Implementation

In this configuration, the values are in a hash and an HNSW index is created. Each entry contains the embedding and the entity's value. A metadata hash is also stored in Redis that allows Redis to maintain its own state. This is used in conjunction with Featureform's Etcd service to achieve consistency between the two.&#x20;

## Configuration

**The RedisSearch module is required to use Redis as a Vector DB**

First we have to add a declarative Redis configuration in Python. In the following example, only name is required, but the other parameters are available.

{% code title="redis_config.py" %}

```python
import featureform as ff

ff.register_redis(
    name = "redis",
    description = "Example inference store",
    team = "Featureform",
    host = "0.0.0.0",
    port = 6379,
    password = "",
    db = 0,
)
```

{% endcode %}

Note that this same provider can also be used as an inference store.

Once our config file is complete, we can apply it to our Featureform deployment

```bash
featureform apply redis_config.py --host $FEATUREFORM_HOST
```

We can re-verify that the provider is created by checking the [Providers tab of the Feature Registry](../getting-started/exploring-the-feature-registry.md).

### Mutable Configuration Fields

* `description`
* `password`

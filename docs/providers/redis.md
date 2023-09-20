# Redis

**The RedisSearch module is required to use Redis as a Vector DB**

Featureform supports [Redis](https://redis.io/) as an [Inference Store](inference-store.md) and a [Vector DB](vector-db.md)

## Implementation

In the inference store configuration, one Redis hash is created per feature. It maps entities to their feature value. A metadata hash is also stored in Redis that allows Redis to maintain its own state. This is used in conjunction with Featureform's Etcd service to achieve consistency between the two.

## Configuration

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

client.apply()
```

{% endcode %}

Once our config file is complete, we can apply it to our Featureform deployment. Afterwards we can set it as the [Inference Store](inference-store.md) or [Vector DB](vector-db.md) when defining a [feature](../abstractions/feature.md) or [embedding](../abstractions/embedding.md) respectively.

We can re-verify that the provider is created by checking the [Providers tab of the Feature Registry or via the CLI](../getting-started/search/monitor-discovery-feature-registry-ui-cli.md).

### Mutable Configuration Fields

* `description`
* `password`

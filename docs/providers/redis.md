# Redis

Featureform supports [Redis](https://redis.io/) as an Inference Store.

## Implementation

In this configuration, one Redis hash is created per feature. It maps entities to their feature value. A metadata hash is also stored in Redis that allows Redis to maintain its own state. This is used in conjunction with Featureform's Etcd service to achieve consistency between the two.&#x20;

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
```
{% endcode %}

Once our config file is complete, we can apply it to our Featureform deployment

```bash
featureform apply redis_config.py --host $FEATUREFORM_HOST
```

We can re-verify that the provider is created by checking the [Providers tab of the Feature Registry](../getting-started/exploring-the-feature-registry.md).

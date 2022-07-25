# Cassandra

Featureform supports [Cassandra](https://cassandra.apache.org/\_/index.html) as an Inference Store.

## Implementation

A Cassandra table is created for every feature. All these tables are part of a keyspace with replication set to 3 by default. A metadata table exists within the keyspace as well to allow the provider to keep track of its own state. Featureform's scheduler aims to achieve consistency between Cassandra's internal state with the user's desired state as specified in the metadata service.

## Configuration

First we have to add a declarative Cassandra configuration in Python. In the following example,  only name is required, but the other parameters are available.

{% code title="cassandra_config.py" %}
```python
import featureform as ff
ff.register_cassandra(
    name = "cassandra",
    description = "Example inference store",
    team = "Featureform",
    host = "0.0.0.0",
    port = 9042,
    username = "cassandra",
    password = "cassandra",
    consistency = "THREE",
    replication = 3
)
```
{% endcode %}

Once our config file is complete, we can apply it to our Featureform deployment

```bash
featureform apply cassandra_config.py --host $FEATUREFORM_HOST
```

We can re-verify that the provider is created by checking the [Providers tab of the Feature Registry](../getting-started/exploring-the-feature-registry.md).

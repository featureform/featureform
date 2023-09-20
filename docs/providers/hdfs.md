# Azure Blobs

Featureform supports [HDFS](https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html) as a [File Store](object-and-file-stores.md)

## Configuration

First we have to add a declarative HDFS configuration in Python.

{% code title="hdfs_config.py" %}

```python
import featureform as ff

hdfs = ff.register_hdfs(
    name="hdfs-quickstart",
    host="<host>",
    port="<port>",
    path="<path>",
    username="<username>",
    description="An hdfs store provider to store offline"
)

client.apply()
```

{% endcode %}

We can re-verify that the provider is created by checking the [Providers tab of the Feature Registry or via the CLI](../getting-started/search-monitor-discovery-feature-registry-ui-cli.md).

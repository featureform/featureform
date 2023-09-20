# Azure Blobs

Featureform supports [Google Cloud Storage (GCS)](https://cloud.google.com/storage) as a [File Store](object-and-file-store.md)

## Configuration

First we have to add a declarative GCS configuration in Python.

{% code title="gcs_config.py" %}

```python
import featureform as ff

gcs = ff.register_gcs(
    name="gcs-quickstart",
    credentials=ff.GCPCredentials(...),
    bucket_name="bucket_name",
    bucket_path="featureform/path/",
    description="An gcs store provider to store offline"
)

client.apply()
```

{% endcode %}

We can re-verify that the provider is created by checking the [Providers tab of the Feature Registry or via the CLI](../getting-started/search/monitor-discovery-feature-registry-ui-cli.md).

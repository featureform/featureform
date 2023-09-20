# Azure Blobs

Featureform supports [AWS S3](https://aws.amazon.com/s3/) as a [File Store](object-and-file-store.md)

## Configuration

First we have to add a declarative S3 configuration in Python.

{% code title="s3_config.py" %}

```python
import featureform as ff

s3 = ff.register_s3(
    name="s3-quickstart",
    credentials=ff.AWSCredentials(...),
    bucket_name="bucket_name",
    bucket_region=<bucket_region>,
    path="path/to/store/featureform_files/in/",
    description="An s3 store provider to store offline"
)

client.apply()
```

{% endcode %}

We can re-verify that the provider is created by checking the [Providers tab of the Feature Registry or via the CLI](../getting-started/search/monitor-discovery-feature-registry-ui-cli.md).

# Azure Blobs

Featureform supports [Azure Blob Store](https://azure.microsoft.com/en-us/products/storage/blobs/) as a [File Store](object-and-file-stores.md)

## Configuration

First we have to add a declarative Azure Blob configuration in Python.

{% code title="azure_blob_config.py" %}

```python
import featureform as ff

azure_blob = ff.register_blob_store(
    name="azure-quickstart",
    description="An azure blob store provider to store offline and inference data" # Optional
    container_name="my_company_container"
    root_path="custom/path/in/container"
    account_name="<azure_account_name>"
    account_key="<azure_account_key>" 
)

client.apply()
```

{% endcode %}

We can re-verify that the provider is created by checking the [Providers tab of the Feature Registry or via the CLI](../getting-started/search-monitor-discovery-feature-registry-ui-cli.md).

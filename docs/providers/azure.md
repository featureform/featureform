# Azure Blobs

Featureform supports [Azure Blob Store](https://azure.microsoft.com/en-us/products/storage/blobs/) as an Inference Store and Storage layer for a Kubernetes Offline Store.

## Implementation

An Azure Blob created for every feature. The data type is stored in an index, and the values stored in a keyspace based on their entity. Featureform's scheduler aims to achieve consistency between Azure's internal state with the user's desired state as specified in the metadata service.

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
```
{% endcode %}

Once our config file is complete, we can apply it to our Featureform deployment

```bash
featureform apply azure_blob_config.py --host $FEATUREFORM_HOST
```

We can re-verify that the provider is created by checking the [Providers tab of the Feature Registry](../getting-started/exploring-the-feature-registry.md).

## Kubernetes Offline Store

[Kubernetes serves as a compute layer](kubernetes.md) for generating training sets, SQL, and Dataframe transformations. To use Kubernetes, a storage layer to store the results of the computation needs to be specified.

{% code title="azure_blob_config.py" %}
```python
import featureform as ff
k8s_store = ff.register_k8s(
    name="k8s",
    description="Native featureform kubernetes compute",
    store=azure_blob,
    team="featureform-team"
)
```
{% endcode %}
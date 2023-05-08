# Kubernetes

Featureform supports [Kubernetes](https://kubernetes.io/) as an Offline Store.

## Implementation <a href="#implementation" id="implementation"></a>

Since featureform is deployed natively on a Kubernetes cluster, it can leverage its own cluster to compute transformations and training sets. Featureform however does not handle storage in non-local mode, so it is necessary to separately register a file store provider like [Azure](azure.md) to store the results of its computation.

## Requirements

* [Remote file storage (eg. Azure Blob Storage)](azure.md)

### Transformation Sources

Using Kubernetes as an Offline Store, you can [define new transformations](../getting-started/transforming-data.md) via [SQL and Pandas DataFrames](https://spark.apache.org/docs/latest/sql-programming-guide.html). Using either these transformations or preexisting tables in your file store, a user can chain transformations and register columns in the resulting tables as new features and labels.

### Training Sets and Inference Store Materialization

Any column in a preexisting table or user-created transformation can be registered as a feature or label. These features and labels can be used, as with any other Offline Store, for [creating training sets and inference serving.](../getting-started/defining-features-labels-and-training-sets.md)

## Configuration <a href="#configuration" id="configuration"></a>

To configure a Kubernetes store as a provider, you merely need to have featureform running in your Kubernetes cluster, and register a compatible file store to store the output of the computation. Featureform automatically downloads and uploads the necessary files to handle all necessary functionality of a native offline store like Postgres or BigQuery.

{% code title="k8s_quickstart.py" %}

```python
import featureform as ff

azure_blob = ff.register_blob_store(
    name="azure-quickstart",
    description="An azure blob store provider to store offline and inference data", # Optional
    container_name="my_company_container",
    root_path="custom/path/in/container",
    account_name="<azure_account_name>",
    account_key="<azure_account_key>"
)

k8s_store = ff.register_k8s(
    name="k8s",
    description="Native featureform kubernetes compute",
    store=azure_blob,
    team="featureform-team"
)
```

{% endcode %}

### Mutable Configuration Fields

* `description`
* `docker_image`

For your file store provider documentation for its mutable fields.

### Dataframe Transformations

Using your Kubernetes as a provider, a user can define transformations in SQL like with other offline providers.

{% code title="k8s_quickstart.py" %}

```python
transactions = k8s_store.register_file(
    name="transactions",
    variant="kaggle",
    owner="featureformer",
    file_path="source_datasets/transaction_short.csv",
)

@k8s_store.sql_transformation()
def max_transaction_amount():
    """the average transaction amount for a user """
    return "SELECT CustomerID as user_id, max(TransactionAmount) " \
        "as max_transaction_amt from {{transactions.kaggle}} GROUP BY user_id"
```

{% endcode %}

In addition, registering a provider via Kubernetes allows you to perform DataFrame transformations using your source tables as inputs.

{% code title="k8s_quickstart.py" %}

```python
@k8s_store.df_transformation(
    inputs=[("transactions", "kaggle")], variant="default")
def average_user_transaction(transactions):
    user_tsc = transactions[["CustomerID","TransactionAmount","Timestamp"]]
    return user_tsc.groupby("CustomerID").agg({'TransactionAmount':'mean','Timestamp':'max'})
```

{% endcode %}

## Custom Compute Images

By default, the Docker image used to run compute for the transformations only has Pandas pre-installed. However, custom images can be built using Featureform's base image to enable use of other 3rd party libraries.

### Building A Custom Image

Custom images can be built and stored in your own repository, as long as the Kubernetes cluster has permission to access
that repository.

You can install additional python packages on top of the base Featureform image.

{% code title="Dockerfile" %}

```dockerfile
FROM featureformcom/k8s_runner:latest
RUN pip install scikit-learn
```

{% endcode %}

### Using A Custom Image (Provider-Wide)

Once you've built your custom image and pushed it to your docker repository, you can use it in your Featureform cluster.

To use the custom-built image, you can add it to the Kubernetes Provider registration. This will override the default image for all jobs run with this provider.

{% code title="k8s_quickstart.py" %}

```python
k8s_custom = ff.register_k8s(
    name="k8s-custom",
    description="Native featureform kubernetes compute",
    store=azure_blob,
    team="featureform-team",
    docker_image="my-repo/my-image:latest"
)
```

{% endcode %}

To use these libraries in a transformation, you can import them within the transformation definition.

{% code title="k8s_quickstart.py" %}

```python
@k8s_custom.df_transformation(inputs=[("encode_product_category", "default")])
def add_kmeans_clustering(df):
    from sklearn.cluster import KMeans
    kmeans = KMeans(n_clusters=6)
    df["Cluster"] = kmeans.fit_predict(df)
    df["Cluster"] = X["Cluster"].astype("category")
    return df
```

{% endcode %}

### Using A Custom Image (Per-Transformation)

Custom images can also be used per-transformation. The specified image will override the default image or provider-wide
image for only the transformation it is specified in.

{% code title="k8s_quickstart.py" %}

```python
@k8s_custom.df_transformation(
    inputs=[("encode_product_category", "default")],
    docker_image="my-repo/my-image:latest"
)
def add_kmeans_clustering(df):
    from sklearn.cluster import KMeans
    kmeans = KMeans(n_clusters=6)
    df["Cluster"] = kmeans.fit_predict(df)
    df["Cluster"] = X["Cluster"].astype("category")
    return df
```

{% endcode %}

## Custom Resource Requests and Limits

By default, transformation pods will be scheduled without resource requests or limits. This means that the pods will be 
scheduled on any node that has available resources.

You can specify resource requests and limits for the transformation pods. This will ensure that the pods are scheduled
when the appropriate resources are available.

```python
specs = ff.K8sResourceSpecs(
    cpu_request="100m",
    cpu_limit="500m",
    memory_request="1Gi",
    memory_limit="2Gi"
)

@k8s_store.df_transformation(
    inputs=[("transactions", "kaggle")], 
    variant="default", 
    resource_specs=specs
)
def average_user_transaction(transactions):
    user_tsc = transactions[["CustomerID","TransactionAmount","Timestamp"]]
    return user_tsc.groupby("CustomerID").agg({'TransactionAmount':'mean','Timestamp':'max'})
```

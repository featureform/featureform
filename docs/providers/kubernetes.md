# Kubernetes

Featureform supports [Kubernetes](https://kubernetes.io/) as an Offline Store.

## Implementation <a href="#implementation" id="implementation"></a>
Since featureform is deployed natively on a Kubernetes cluster, it can leverage its own cluster to compute transformations and training sets. Featureform however does not handle storage in non-local mode, so it is necessary to seperately register a file store provider like [Azure](azure.md) to store the results of its computation.

#### Requirements
* [Remote file storage (eg. Azure Blob Storage)](azure.md)

### Transformation Sources

Using Kubernetes as an Offline Store, you can [define new transformations](../getting-started/transforming-data.md) via [SQL and Pandas DataFrames](https://spark.apache.org/docs/latest/sql-programming-guide.html). Using either these transformations or preexisting tables in your file store, a user can chain transformations and register columns in the resulting tables as new features and labels.

### Training Sets and Inference Store Materialization

Any column in a preexisting table or user-created transformation can be registered as a feature or label. These features and labels can be used, as with any other Offline Store, for [creating training sets and inference serving.](../getting-started/defining-features-labels-and-training-sets.md)

## Configuration <a href="#configuration" id="configuration"></a>

To configure a Kubernetes store as a provider, you merely need to have featureform running in your Kubernetes cluster, and register a compatible file store to store the ouput of the computation. Featureform automatically downloads and uploads the necessary files to handle all necessary functionality of a native offline store like Postgres or BigQuery.


{% code title="k8s_quickstart.py" %}
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

k8s_store = ff.register_k8s(
    name="k8s",
    description="Native featureform kubernetes compute",
    store=azure_blob,
    team="featureform-team"
)
```
{% endcode %}

### Dataframe Transformations
Using your Kubernetes as a provider, a user can define transformations in SQL like with other offline providers.

{% code title="k8s_quickstart.py" %}
```python
transactions = k8s_store.register_parquet_file(
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
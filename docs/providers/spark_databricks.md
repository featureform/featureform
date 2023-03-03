# Spark with Databricks

Featureform supports [Databricks](https://www.databricks.com) as an Offline Store.

## Implementation <a href="#implementation" id="implementation"></a>
With Databricks, you can leverage your Databricks cluster to compute transformations and training sets. Featureform however does not handle storage in non-local mode, so it is necessary to seperately register a file store provider like [Azure](azure.md) to store the results of its computation.

#### Requirements
* Databricks Cluster
* [Remote file storage (eg. Azure Blob Storage)](azure.md)

### Transformation Sources

Using Databricks as an Offline Store, you can [define new transformations](../getting-started/transforming-data.md) via [SQL and Spark DataFrames](https://spark.apache.org/docs/latest/sql-programming-guide.html). Using either these transformations or preexisting tables in your file store, a user can chain transformations and register columns in the resulting tables as new features and labels.

### Training Sets and Inference Store Materialization

Any column in a preexisting table or user-created transformation can be registered as a feature or label. These features and labels can be used, as with any other Offline Store, for [creating training sets and inference serving.](../getting-started/defining-features-labels-and-training-sets.md)

## Configuration <a href="#configuration" id="configuration"></a>

To configure a Databricks store as a provider, you need to have a Databricks cluster. Featureform automatically downloads and uploads the necessary files to handle all necessary functionality of a native offline store like Postgres or BigQuery.


{% code title="databricks_definition.py" %}
```python
import featureform as ff

databricks = ff.DatabricksCredentials(
    # Can either use username/password or host/token.
    username="<username>",
    password="<password>",
    host="<host>",
    token="<token>",
    cluster_id="<cluster_id>"
)

azure_blob = ff.register_blob_store(
    name="azure-quickstart",
    description="An azure blob store provider to store offline and inference data" # Optional
    container_name="my_company_container"
    root_path="custom/path/in/container"
    account_name="<azure_account_name>"
    account_key="<azure_account_key>" 
)

spark = ff.register_spark(
    name="spark_provider",
    executor=databricks,
    filestore=azure_blob
)

```
{% endcode %}

### Dataframe Transformations
Using your Spark with Azure and Databricks as a provider, a user can define transformations in SQL and Dataframes like with other offline providers.

{% code title="databricks_definition.py" %}
```python
transactions = spark.register_file(
    name="transactions",
    variant="kaggle",
    owner="featureformer",
    file_path="abfss://<CONTAINER>@<STORAGE_ACCOUNT>.dfs.core.windows.net/source_datasets/transaction_short.csv",
)

@spark.sql_transformation()
def max_transaction_amount():
    """the average transaction amount for a user """
    return "SELECT CustomerID as user_id, max(TransactionAmount) " \
        "as max_transaction_amt from {{transactions.kaggle}} GROUP BY user_id"
```
{% endcode %}

In addition, registering a provider via Spark with Azure & Databricks allows you to perform DataFrame transformations using your source tables as inputs.

{% code title="databricks_definition.py" %}
```python
@spark.df_transformation(
    inputs=[("transactions", "kaggle")], variant="default")
def average_user_transaction(transactions):
    from pyspark.sql.functions import mean
    transactions = transactions.select("CustomerID","TransactionAmount","Timestamp").groupBy("CustomerID").agg(mean("TransactionAmount"))
    return transactions
```
{% endcode %}
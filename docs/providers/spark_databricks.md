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

## Dataframe Transformations
Because Featureform supports the generic implementation of Spark, transformations written in SQL and Dataframe operations for the different Spark providers will be very similar except for the file_path or table name. 

{% content-ref url="../providers/spark.md" %}
[spark.md](../providers/spark.md)
{% endcontent-ref %}
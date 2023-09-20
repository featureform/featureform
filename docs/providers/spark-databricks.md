# Spark with Databricks

Featureform supports [Databricks](https://www.databricks.com) as an Offline Store.

## Implementation <a href="#implementation" id="implementation"></a>

With Databricks, you can leverage your Databricks cluster to compute transformations and training sets. Featureform however does not handle storage in non-local mode, so it is necessary to separately register a [file store provider](object-and-file-stores.md) like [S3](azure-blob-store.md) to store the results of its computation.

## Requirements

* Databricks Cluster
* [Remote file storage (eg. Azure Blob Storage)](object-and-file-stores.md)

### Required Azure Configurations

#### Azure Blob Storage

If you encounter the following error, or one similar to it, when registering a transformation, you may need to disable certain default container configurations:

```text
Caused by: Operation failed: "This endpoint does not support BlobStorageEvents or SoftDelete. Please disable these account features if you would like to use this endpoint."
```

To disable these configurations, you can navigate to `Home > Storage Accounts > YOUR STORAGE ACCOUNT`, and select "Data Protection" under the "Data Management" section. Then uncheck:

* Enable soft delete for blobs
* Enable soft delete for containers

#### Databricks

If you encounter the following error, or one similar to it, when registering a transformation, you may need to add credentials for your Azure Blob Storage account to your Databricks cluster:

```text
Cannot read the python file abfss://<container_name>@<storage_account_name>/<root_path>/featureform/scripts/spark/offline_store_spark_runner_py.
Please check driver logs for more details.
```

To add Azure Blob Store account credentials to your Databricks cluster, you'll need to:

* Launch your Databricks workspace from the Azure portal (e.g. by clicking "Launch Workspace" from the "Overview" page of your Databricks workspace)
* Select "Compute" from the left-hand menu and click on your cluster
* Click "Edit" and then select "Advanced Options" tab to show the "Spark Config" text input field
* Add the following configuration to the "Spark Config" text input field:

```text
spark.hadoop.fs.azure.account.key.<AZURE BLOB STORAGE SERVICE ACCOUNT NAME>.blob.core.windows.net <AZURE BLOB STORAGE SERVICE ACCOUNT KEY>
```

Once you've clicked "Confirm", your cluster will need to restart before you can apply the transformation again.

## Transformation Sources

Using Databricks as an Offline Store, you can define new transformations via SQL and Spark DataFrames. Using either these transformations or preexisting tables in your file store, a user can chain transformations and register columns in the resulting tables as new features and labels.

## Training Sets and Inference Store Materialization

Any column in a preexisting table or user-created transformation can be registered as a feature or label. These features and labels can be used, as with any other Offline Store, for creating training sets and inference serving.

## Configuration <a href="#configuration" id="configuration"></a>

To configure a Databricks store as a provider, you need to have a Databricks cluster. Featureform automatically downloads and uploads the necessary files to handle all necessary functionality of a native offline store like Postgres or BigQuery.

{% code title="databricks_definition.py" %}

```python
import featureform as ff

databricks = ff.DatabricksCredentials(
    # You can either use username and password ...
    username="<username>",
    password="<password>",
    # ... or host and token
    host="<host>",
    token="<token>",
    cluster_id="<cluster_id>"
)

azure_blob = ff.register_blob_store(
    name="azure-quickstart",
    description="An azure blob store provider to store offline and inference data"
    container_name="my_company_container"
    # Will either be the container name or the container name plus a path if you plan read/write
    # to a specific directory in your container
    root_path="my_company_container/path/to/specific/directory"
    account_name="<azure_storage_service_account_name>"
    account_key="<azure_storage_service_account_key>"
)

spark = ff.register_spark(
    name="spark_provider",
    executor=databricks,
    filestore=azure_blob
)

transactions = spark.register_file(
    name="transactions",
    variant=variant,
    # Must be an absolute path using the abfss:// protocol
    file_path="abfss://<container_name>@<azure_storage_service_account_name>.dfs.core.windows.net/transactions.csv",
)
```

{% endcode %}

## Dataframe Transformations

Because Featureform supports the generic implementation of Spark, transformations written in SQL and Dataframe operations for the different Spark providers will be very similar except for the file_path or table name.

{% content-ref url="../providers/spark.md" %}
[Spark](spark.md)
{% endcontent-ref %}

# Spark 

Featureform supports generic Spark as an Offline Store.

This means that Featureform can handle all flavors of Spark using S3, GCS, Azure Blob Store or HDFS as a backing store.

Common use cases of Featureform with Spark include: 

{% content-ref url="../providers/spark_emr.md" %}
[spark_emr.md](../providers/spark_emr.md)
{% endcontent-ref %}

{% content-ref url="../providers/spark_databricks.md" %}
[spark_databricks.md](../providers/spark_databricks.md)
{% endcontent-ref %}

## Understanding The Different Flavors of Spark <a href="#implementation" id="implementation"></a>

### Spark versus Databricks versus Hosted Databricks 
Spark is a powerful, open-source general computing framework developed for large-scale data processing.

Databricks is a managed data and analytics platform developed on top of Spark.

Both Spark and Databricks can be self-hosted in Kubernetes and other non-cloud implementations, as well as hosted with popular cloud providers such as AWS, Azure, GCP and on Databricks.


### Supported Spark Providers Matrix


## 

### Transformation Sources

Using Spark and a filestore (GCS, Azure Blob Storage, S3, HDFS) as an Offline Store, you can [define new transformations](../getting-started/transforming-data.md) via [SQL and Spark DataFrames](https://spark.apache.org/docs/latest/sql-programming-guide.html). Using either these transformations or pre-existing files in your file store, a user can chain transformations and register columns in the resulting tables as new features and labels.

### Training Sets and Inference Store Materialization

Any column in a preexisting table or user-created transformation can be registered as a feature or label. These features and labels can be used, as with any other Offline Store, for [creating training sets and inference serving.](../getting-started/defining-features-labels-and-training-sets.md)

### Dataframe Transformations
Using Spark with Featureform, a user can define transformations in SQL like with other offline providers.


{% code title="spark_quickstart.py" %}
```python
transactions = spark.register_parquet_file(
   name="transactions",
   variant="kaggle",
   owner="featureformer",
   file_path="<insert_file_path_here",
)


@spark.sql_transformation()
def max_transaction_amount():
   """the average transaction amount for a user """
   return "SELECT CustomerID as user_id, max(TransactionAmount) " \
       "as max_transaction_amt from {{transactions.kaggle}} GROUP BY user_id"
```
{% endcode %}


In addition, registering a provider via Spark allows you to perform DataFrame transformations using your source tables as inputs.

{% code title="spark_quickstart.py" %}
```python
@spark.df_transformation(
   inputs=[("transactions", "kaggle")], variant="default")
def average_user_transaction(df):
   from pyspark.sql.functions import avg
   df.groupBy("CustomerID").agg(avg("TransactionAmount").alias("average_user_transaction"))
   return df
```
{% endcode %}

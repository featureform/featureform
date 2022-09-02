# Spark

Featureform supports [Spark on AWS](https://aws.amazon.com/emr/features/spark/) as an Offline Store.

## Implementation <a href="#implementation" id="implementation"></a>
The AWS Spark Offline store implements [AWS Elastic Map Reduce (EMR)](https://aws.amazon.com/emr/) as a compute layer, and [S3](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html) as a storage layer. The transformations, training sets, and feature definitions a user registers via the Featureform client are stored parquet tables in S3.

Using Spark for computation, Featureform leverages EMR to compute user defined transformations and training sets. The user can author new tables and iterate through training sets directly via the [Featureform CLI](getting-started/interact-with-the-cli.md).

Features registered via the client can be materialized to an Inference Store (ex: [Redis](providers/redis.md)) for real-time feature serving.

#### Requirements
* [AWS S3 Bucket](https://docs.aws.amazon.com/s3/?icmpid=docs_homepage_featuredsvcs)
* [AWS EMR Cluster running Spark >=2.4.8](https://docs.aws.amazon.com/emr/index.html)

### Transformation Sources

Using Spark as an Offline Store, you can [define new transformations](getting-started/transforming-data.md) via [SQL and DataFrames](https://spark.apache.org/docs/latest/sql-programming-guide.html). Using either these transformations or preexisting tables in S3, a user can chain transformations and register columns in the resulting tables as new features and labels.

### Training Sets and Inference Store Materialization

Any column in in a preexisting table or user-created transformation can be registered as a feature or label. These features and labels can be used, as with any other Offline Store, for [creating training sets and inference serving.](getting-started/defining-features-labels-and-training-sets.md)

## Configuration <a href="#configuration" id="configuration"></a>

To configure a Spark provider via AWS, you need an [IAM Role](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles.html) with access to account's EMR cluster and S3 bucket. 

The [AWS access key id and your secret access key](https://docs.aws.amazon.com/general/latest/gr/aws-sec-cred-types.html) are used as credentials when registering your Spark Offline Store.

Your EMR cluster must be running and support [Spark](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark.html).

{% code title="spark_config.py" %}
```python
import featureform as ff

ff.register_snowflake(
    name = "spark_offline_store"
    description = "A spark provider that can create transformations and training sets",
    team = "featureform data team",
    emr_cluster_id = "j-ExampleCluster",
    bucket_path = "example-bucket-path", #excluding the "S3://" prefix
    emr_cluster_region = "us-east-1",
    bucket_region = "us-east-2",
    aws_access_key_id = "<access-key-id>",
    aws_secret_access_key = "<aws-secret-access-key>",
    ):
```
{% endcode %}

### Dataframe Transformations
Using Spark with Featureform, a user can define transformations in SQL like with other offline providers.

{% code title="sql_transformation.py" %}
```python
@spark.sql_transformation()
def max_transaction_amount():
    """the average transaction amount for a user """
    return "SELECT CustomerID as user_id, " \
    "max(TransactionAmount) " "as max_transaction_amt " \
    "from {{transactions.kaggle}} GROUP BY user_id"
```
{% endcode %}

In addition, registering a provider via Spark allows you to perform DataFrame transformations using your source tables as inputs.

{% code title="dataframe_transformation.py" %}
```python
@spark.df_transformation(
    inputs=[("transactions", "kaggle")], 
    variant="default")
def average_user_transaction(df):
    from pyspark.sql.functions import avg
        df.groupBy("CustomerID")
        .agg(avg("TransactionAmount")
        .alias("average_user_transaction"))
    return df
```
{% endcode %}

These transformations are cross compatable. SQL and DataFrame transformations, identified via their name and variants, can be used as inputs to one another.

{% code title="sql_with_dataframe_source.py" %}
```python
@spark.sql_transformation()
def average_user_transaction():
    return "SELECT * FROM {{average_user_transaction.default}} " \
        "INNER JOIN {{max_transaction_amount}} ON " \
        "{{average_user_transaction.default}}.CustomerID = " \
        "{{max_transaction_amount.default}}.user_id"
```
{% endcode %}

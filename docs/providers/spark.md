# Spark

Featureform supports [Spark on AWS](https://aws.amazon.com/emr/features/spark/) as an Offline Store

## Implementation <a href="#implementation" id="implementation"></a>
Featureform implements spark as a transformation layer. The results of the transformations and table creations are stored in S3.

Using the client, a user can define transformtions using dataframes, which transform into new tables. The function name and decorator variant label can be used as an identifier to further chain transformations, and use the new tables to create training sets.

Current support requires an [AWS EMR](https://aws.amazon.com/emr/) cluster and an [S3](https://aws.amazon.com/s3/) bucket to store the featureform generated tables, feature definitions and training sets.
Features can be copied from spark to an inference store (ex: Redis)(link to page) for real-time feature serving

Data is stored in parquet tables in the bucket. The spark store executes a spark job on your cluster to transform the data and copy it to its relevant sources for training and serving

#### Requirements
	Amazon AWS account with:
		AWS S3 Bucket
		AWS EMR Cluster
			Runs Spark version >2

### Transformation Sources

SQL transformations are used to create a view. By default, those views are materialized and updated according to the schedule parameter. Deprecated transformations are converted to un-materialized views to save storage space.

### Offline to Inference Store Materialization

When a feature is registered, the featureform spark store creates an internal link to the relevant columns pertaining to that feature in its source. A kubernetes job transfers the latest values from these columns to sync to the inference store

### Training Set Generation

Every registered feature and label is associated specific columns in a parquet table. That view contains three columns, the entity, value, and timestamp. When a training set is registered, it is created new parquet table in the bucket, via a JOIN on the corresponding label and features from the source tables.

## Configuration <a href="#configuration" id="configuration"></a>

To configure a spark store via aws, you need an [IAM Role](https://aws.amazon.com/iam/) with access to an account's EMR cluster and S3 bucket. To register you can input your [aws access key id and your aws secret key](https://docs.aws.amazon.com/general/latest/gr/aws-sec-cred-types.html).
Your cluster must be running and support [Spark](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark.html).

{% code title="spark_config.py" %}
```python
import featureform as ff

ff.register_snowflake(
    name = "spark_offline_store"
    description = "A spark provider that can create transformations and training sets",
    team = "featureform data team",
    emr_cluster_id = "j-ExampleCluster",
    bucket_path = "example-bucket-path", //excluding the "S3://" prefix
    emr_cluster_region = "us-east-1",
    bucket_region = "us-east-2",
    aws_access_key_id = "<access-key-id>",
    aws_secret_access_key = "<aws-secret-access-key>",):
```
{% endcode %}

### Dataframe Transformations
While you can also create SQL transformations as in the other offline stores like [Redis](providers/redis.md), Spark uniquely allows you to perform dataframe transformations using your source tables as inputs.
with your registered spark store, you can perform
{% code title="dataframe_registration.py" %}
```python
@spark.df_transformation(inputs=[("transactions", "kaggle")], variant="default")        # Sources are added as inputs
def average_user_transaction(df):                           # Sources can be manipulated by adding them as params
    from pyspark.sql.functions import avg
    df.groupBy("CustomerID").agg(avg("TransactionAmount").alias("average_user_transaction"))
    return df

@spark.sql_transformation()        # Sources are added as inputs
def max_transaction_amount():                           # Sources can be manipulated by adding them as params
    """the average transaction amount for a user """
    return "SELECT CustomerID as user_id, max(TransactionAmount) " \
           "as max_transaction_amt from {{transactions.kaggle}} GROUP BY user_id"
```
{% endcode %}
and use the result of your transformation as the input to another one
{% code title="dataframe_registration.py" %}
```python
@spark.sql_transformation()
def average_user_transaction():                           # Sources can be manipulated by adding them as params
    "SELECT * FROM {{average_user_transaction.default}} INNER JOIN {{max_transaction_amount}} ON " \
    "{{average_user_transaction.default}}.CustomerID = {{max_transaction_amount.default}}.user_id"
```
{% endcode %}

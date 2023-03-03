# Spark with EMR

Featureform supports [Spark on AWS](https://aws.amazon.com/emr/features/spark/) as an Offline Store.

## Implementation <a href="#implementation" id="implementation"></a>
The AWS Spark Offline store implements [AWS Elastic Map Reduce (EMR)](https://aws.amazon.com/emr/) as a compute layer, and [S3](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html) as a storage layer. The transformations, training sets, and feature definitions a user registers via the Featureform client are stored as parquet tables in S3.

Using Spark for computation, Featureform leverages EMR to compute user defined transformations and training sets. The user can author new tables and iterate through training sets sourced directly from S3 via the [Featureform CLI](../getting-started/interact-with-the-cli.md).

Features registered on the Spark Offline Store can be materialized to an Inference Store (ex: [Redis](./redis.md)) for real-time feature serving.

#### Requirements
* [AWS S3 Bucket](https://docs.aws.amazon.com/s3/?icmpid=docs_homepage_featuredsvcs)
* [AWS EMR Cluster running Spark >=2.4.8](https://docs.aws.amazon.com/emr/index.html)



## Configuration <a href="#configuration" id="configuration"></a>

To configure a Spark provider via AWS, you need an [IAM Role](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles.html) with access to account's EMR cluster and S3 bucket. 

Your [AWS access key id and AWS secret access key](https://docs.aws.amazon.com/general/latest/gr/aws-sec-cred-types.html) are used as credentials when registering your Spark Offline Store.

Your EMR cluster must be running and support [Spark](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark.html). 

The EMR cluster, before being deployed, must run a bootstrap action to install the necessary python pacakges to run Featureform's Spark script. The following link contains the script that must be added as a bootstrap action for your cluster to be comptatible with Featureform:

[https://featureform-demo-files.s3.amazonaws.com/python_packages.sh](https://featureform-demo-files.s3.amazonaws.com/python_packages.sh)


{% code title="spark_quickstart.py" %}
```python
import featureform as ff

emr = EMRCredentials(
    aws_access_key_id="<aws_access_key_id>",
    aws_secret_access_key="<aws_secret_access_key>",
    emr_cluster_id="<emr_cluster_id>",
    emr_cluster_region="<emr_cluster_id>"
)

s3 = ff.register_s3(
    name="s3",
    credentials="<aws_creds>",
    bucket_path="<s3_bucket_path>",
    bucket_region="<s3_bucket_region>"
)

spark = ff.register_spark(
    name="<spark-emr-s3>",
    description="A Spark deployment we created for the Featureform quickstart",
    team="<team_name>",
    executor=emr,
    filestore=s3,
)
```
{% endcode %}

## Dataframe Transformations
Because Featureform supports the generic implementation of Spark, transformations written in SQL and Dataframe operations for the different Spark providers will be very similar except for the file_path or table name. 


Examples of Dataframe transformations for both SQL and Dataframe operations can be found in the main Spark providers page.

{% content-ref url="../providers/spark.md" %}
[spark.md](../providers/spark.md)
{% endcontent-ref %}

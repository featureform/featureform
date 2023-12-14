# Providers

[//]: # (## Capability Matrix)

[//]: # ()
[//]: # (|                         Name                          | Offline | Online | Compute | Storage | Vector | Available in Localmode |)

[//]: # (|:-----------------------------------------------------:|:-------:|:------:|:-------:|:-------:|:------:|------------------------|)

[//]: # (|         [Azure Blob Store]&#40;#azure-blob-store&#41;         |    x    |        |         |    x    |        |                        |)

[//]: # (|                 [BigQuery]&#40;#bigquery&#41;                 |    x    |        |    x    |    x    |        |                        |)

[//]: # (|                [Cassandra]&#40;#cassandra&#41;                |         |   x    |         |    x    |        |                        |)

[//]: # (|                 [DynamoDB]&#40;#dynamodb&#41;                 |         |   x    |         |    x    |        |                        |)

[//]: # (|                [Firestore]&#40;#firestore&#41;                |         |   x    |         |    x    |        |                        |)

[//]: # (|     [Google Cloud Storage]&#40;#google-cloud-storage&#41;     |    x    |        |         |    x    |        |                        |)

[//]: # (|                     [HDFS]&#40;#hdfs&#41;                     |    x    |        |         |    x    |        |                        |)

[//]: # (| [Kubernetes Pandas Runner]&#40;#kubernetes-pandas-runner&#41; |    x    |        |    x    |         |        |                        |)

[//]: # (|           [Local Provider]&#40;#local-provider&#41;           |    x    |        |    x    |    x    |        | x                      |)

[//]: # (|                  [MongoDB]&#40;#mongodb&#41;                  |         |   x    |         |    x    |        |                        |)

[//]: # (|                 [Pinecone]&#40;#pinecone&#41;                 |         |   x    |         |    x    |   x    | x                      |)

[//]: # (|                 [Postgres]&#40;#postgres&#41;                 |    x    |        |    x    |    x    |        |                        |)

[//]: # (|                    [Redis]&#40;#redis&#41;                    |         |   x    |         |    x    |   x    |                        |)

[//]: # (|                 [Redshift]&#40;#redshift&#41;                 |    x    |        |    x    |    x    |        |                        |)

[//]: # (|                       [S3]&#40;#s3&#41;                       |    x    |        |         |    x    |        |                        |)

[//]: # (|                [Snowflake]&#40;#snowflake&#41;                |    x    |        |    x    |    x    |        |                        |)

[//]: # (|                    [Spark]&#40;#spark&#41;                    |    x    |        |    x    |         |        |                        |)

[//]: # (|                 [Weaviate]&#40;#weaviate&#41;                 |         |   x    |         |    x    |   x    | x                      |)

## Credentials
Credentials are objects that can be reused in the same definitions file when registering providers in the same cloud.

### Cloud Providers

#### AWS
::: featureform.resources.AWSCredentials.__init__
    handler: python
    options:
        show_root_heading: false
        show_source: false
        show_root_toc_entry: false

#### Google Cloud
::: featureform.resources.GCPCredentials.__init__
    handler: python
    options:
        show_root_heading: false
        show_source: false
        show_root_toc_entry: false

### Spark
#### Generic
::: featureform.resources.SparkCredentials.__init__
    handler: python 
    options:
        show_root_heading: false
        show_source: false
        show_root_toc_entry: false


#### Databricks
::: featureform.resources.DatabricksCredentials
    handler: python
    options:
        show_root_heading: false
        show_source: false
        show_root_toc_entry: false

#### EMR
::: featureform.resources.EMRCredentials.__init__
    handler: python
    options:
        show_root_heading: false
        show_source: false
        show_root_toc_entry: false

## Provider Registration
This page provides reference and examples for how to register the various providers that Featureform supports.

### Azure Blob Store
::: featureform.register.Registrar.register_blob_store
    handler: python
    options:
        show_root_heading: false
        show_source: false
        show_root_toc_entry: false

### BigQuery
::: featureform.register.Registrar.register_bigquery
    handler: python
    options:
        show_root_heading: false
        show_source: false
        show_root_toc_entry: false

### Cassandra
::: featureform.register.Registrar.register_cassandra
    handler: python
    options:
        show_root_heading: false
        show_source: false
        show_root_toc_entry: false

### DynamoDB
::: featureform.register.Registrar.register_dynamodb
    handler: python
    options:
        show_root_heading: false
        show_source: false
        show_root_toc_entry: false

### Firestore
::: featureform.register.Registrar.register_firestore
    handler: python
    options:
        show_root_heading: false
        show_source: false
        show_root_toc_entry: false

### Google Cloud Storage
::: featureform.register.Registrar.register_gcs
    handler: python
    options:
        show_root_heading: false
        show_source: false
        show_root_toc_entry: false

### HDFS
::: featureform.register.Registrar.register_hdfs
    handler: python
    options:
        show_root_heading: false
        show_source: false
        show_root_toc_entry: false

### Kubernetes Pandas Runner
::: featureform.register.Registrar.register_k8s
    handler: python
    options:
        show_root_heading: false
        show_source: false
        show_root_toc_entry: false

### Local Provider
::: featureform.register.Registrar.register_local
    handler: python
    options:
        show_root_heading: false
        show_source: false
        show_root_toc_entry: false

### MongoDB
::: featureform.register.Registrar.register_mongodb
    handler: python
    options:
        show_root_heading: false
        show_source: false
        show_root_toc_entry: false

### Pinecone
::: featureform.register.Registrar.register_pinecone
    handler: python
    options:
        show_root_heading: false
        show_source: false
        show_root_toc_entry: false

### Postgres
::: featureform.register.Registrar.register_postgres
    handler: python
    options:
        show_root_heading: false
        show_source: false
        show_root_toc_entry: false

### Redis
::: featureform.register.Registrar.register_redis
    handler: python
    options:
        show_root_heading: false
        show_source: false
        show_root_toc_entry: false

### Redshift
::: featureform.register.Registrar.register_redshift
    handler: python
    options:
        show_root_heading: false
        show_source: false
        show_root_toc_entry: false

### S3
::: featureform.register.Registrar.register_s3
    handler: python
    options:
        show_root_heading: false
        show_source: false
        show_root_toc_entry: false

### Snowflake
#### Current
::: featureform.register.Registrar.register_snowflake
    handler: python
    options:
        show_root_heading: false
        show_source: false
        show_root_toc_entry: false

#### Legacy
::: featureform.register.Registrar.register_snowflake_legacy
    handler: python
    options:
        show_root_heading: false
        show_source: false
        show_root_toc_entry: false

### Spark
::: featureform.register.Registrar.register_spark
    handler: python
    options:
        show_root_heading: false
        show_source: false
        show_root_toc_entry: false

### Weaviate
::: featureform.register.Registrar.register_weaviate
    handler: python
    options:
        show_root_heading: false
        show_source: false
        show_root_toc_entry: false
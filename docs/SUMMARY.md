# Summary

## Overview

* [What is Featureform](README.md)
* [Quickstart](quickstart.md)
* [Concepts](concepts/concepts.md)
    * [Immutablility, Lineage, and DAGs](concepts/immutability-lineage-and-dags.md)
    * [Versioning and Variants](concepts/versioning-and-variants.md)
    * [Setting Custom Tags and Properties](concepts/tags-and-properties.md)
    * [Streaming Data: Real-time Updates](concepts/streaming.md)
    * [Interacting with Resources as Dataframes](concepts/dataframes.md)
    * [Dataframe and SQL Transformation Support](concepts/dataframe-sql-transformation.md)
    * [RAG, Vector DBs, and LLMs](concepts/rag-vector-db-llms.md)
    * [Caculating On-Demand Features at Request Time](concepts/on-demand-features-request-time.md)
    * [Search and Discover Features and Transformations](concepts/search-and-discovery-features.md)
    * [Point-in-Time Correctness and Historical Features with Timeseries Data](concepts/point-in-time-correctness-historical-features-timeseries-data.md)
    * [Governance and Access Control](concepts/governance-and-access-control.md)
    * [Enterprise vs Open-Source](concepts/enterprise-vs-open-source-feature-store.md)
* [Python SDK Reference](https://sdk.featureform.com)

## Using Featureform

* [Featureform Workflow](getting-started/featureform-workflow.md)
* [Featureform Architecture & Components](getting-started/architecture-and-components.md)
* [Connect your Data Infrastructure Providers](getting-started/connecting-your-data-infrastructure.md)
* [Register, Transform, and Interact with Data Sets](getting-started/registering-transforming-and-interacting-with-data-sets.md)
    * [Scheduling Transformations](getting-started/scheduling-resources.md)
    * [Using Streaming Data](getting-started/streaming-features.md)
    * [Request Time Transformations](getting-started/on-demand-features-request-time.md)
* [Register and Serve Features and Training Sets](getting-started/register-and-serve-features-training-sets.md)
    * [Tracking Model to Feature Lineage](getting-started/model-to-feature-lineage.md)
    * [On-Demand Features](getting-started/on-demand-features-request-time.md)
    * [Streaming Features](getting-started/streaming-features.md)
* [Search, Monitor, and Discover with the Feature Registry UI and CLI](getting-started/search-monitor-discovery-feature-registry-ui-cli.md)

## Featureform Resource Types

* [Resource Types](abstractions/abstractions.md)
* [Data Infrastructure Provider](abstractions/data-infrastructure-providers.md)
* [Primary Data Sets](abstractions/primary-data-sets.md)
* [Transforming Data Sets](abstractions/transforming-data-sets.md)
* [Entity](abstractions/entity.md)
* [Feature](abstractions/feature.md)
* [Embedding](abstractions/embedding.md)
* [Label](abstractions/label.md)
* [Training Set](abstractions/training-set.md)

## LLMs, Embeddings, and Vector Databases

* [LLM Workflow](embeddings/llm-workflow-with-featureform.md)
* [Building a Chatbot with OpenAI and a Vector Database](embeddings/building-a-chatbot-with-openai-and-a-vector-database.md)


## Common Use Cases and Examples

* [Fraud Detection](use-cases/fraud-detection.md)
* [LLM Chatbot with Private Data & RAG](use-cases/llm-chatbot-rag.md)

## Supported Infrastructure Providers

* [Overview](providers/overview.md)
* [Offline Stores](providers/offline-store.md)
    * [Snowflake](providers/snowflake.md)
    * [Spark](providers/spark.md)
    * [Spark with Databricks](providers/spark-databricks.md)
    * [Spark with EMR](providers/spark-emr.md)
    * [Pandas on Kubernetes](providers/pandas-on-kubernetes.md)
    * [BigQuery](providers/bigquery.md)
    * [Postgres](providers/postgres.md)
    * [Redshift](providers/redshift.md)
* [Object and File Stores](providers/object-and-file-stores.md)
    * [AWS S3](providers/aws-s3.md) 
    * [Azure Blob Store](providers/azure-blob-store.md) 
    * [Google Cloud GCS](providers/google-cloud-gcs.md) 
    * [HDFS](providers/hdfs.md)
* [Inference Stores](providers/inference-store.md)
   * [Cassandra](providers/cassandra.md)
   * [DynamoDB](providers/dynamodb.md)
   * [Firestore](providers/firestore.md)
   * [MongoDB](providers/mongodb.md)
   * [Redis](providers/redis.md)
* [Vector Databases](providers/vector-db.md)
   * [Redis](providers/redis.md)
   * [Pinecone](providers/pinecone.md)
   * [Weaviate](providers/weaviate.md)
* [Custom Providers and Provider Requests](providers/custom-providers.md)

## Deployment

* [Local](deployment/local-mode.md)
* [Stand-alone Docker Container](deployment/quickstart-docker.md)
* [Kubernetes](deployment/kubernetes.md)
* [AWS](deployment/quickstart-aws.md)
* [Azure](deployment/quickstart-azure.md)
* [Google Cloud](deployment/quickstart-gcp.md)
* [System Architecture](deployment/system-architecture.md)
* [Backup and Restore](deployment/backup-and-restore.md)

***


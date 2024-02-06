# Reusing Providers
Featureform's API allows you to reuse already applied definitions. You can easily get pre-applied providers and
resources to continue building off of.

To reuse a provider, simply use the associated `get` method for that provider. 

## Example

```python
from featureform as ff
postgres = ff.get_postgres("prod-instance")

postgres.register_table(
    name="transactions",
    variant="2022",
    table="2022_transactions",
)
```

## Available Providers

[//]: # (We're missing a couple of providers here. Needs to be resolved)

### BigQuery
::: featureform.register.Registrar.get_bigquery
    handler: python
    options:
        show_root_heading: false
        show_source: false
        show_root_toc_entry: false

### K8s Runner
::: featureform.register.Registrar.get_kubernetes
    handler: python
    options:
        show_root_heading: false
        show_source: false
        show_root_toc_entry: false

### MongoDB
::: featureform.register.Registrar.get_mongodb
    handler: python
    options:
        show_root_heading: false
        show_source: false
        show_root_toc_entry: false

### Postgres
::: featureform.register.Registrar.get_postgres
    handler: python
    options:
        show_root_heading: false
        show_source: false
        show_root_toc_entry: false

### ClickHouse
::: featureform.register.Registrar.get_clickhouse
    handler: python
    options:
        show_root_heading: false
        show_source: false
        show_root_toc_entry: false

### Redis
::: featureform.register.Registrar.get_redis
    handler: python
    options:
        show_root_heading: false
        show_source: false
        show_root_toc_entry: false

### Redshift
::: featureform.register.Registrar.get_redshift
    handler: python
    options:
        show_root_heading: false
        show_source: false
        show_root_toc_entry: false

### S3
::: featureform.register.Registrar.get_s3
    handler: python
    options:
        show_root_heading: false
        show_source: false
        show_root_toc_entry: false

### Snowflake
::: featureform.register.Registrar.get_snowflake
    handler: python
    options:
        show_root_heading: false
        show_source: false
        show_root_toc_entry: false

### Spark
::: featureform.register.Registrar.get_spark
    handler: python
    options:
        show_root_heading: false
        show_source: false
        show_root_toc_entry: false


# Postgres

Featureform supports [Postgres](https://www.postgresql.org/) as an Offline Store.

## Implementation <a href="#implementation" id="implementation"></a>

### Primary Sources

#### Tables

Table sources are used directly via a view. Featureform will never write to a primary source.

### Transformation Sources

SQL transformations are used to create a view. By default, those views are materialized and updated according to the schedule parameter. Deprecated transformations are converted to un-materialized views to save storage space.

### Offline to Inference Store Materialization

When a feature is registered, Featureform creates an internal transformation to get the newest value of every feature and its associated entity. A Kubernetes job is then kicked off to sync this up with the Inference store.

### Training Set Generation

Every registered feature and label is associated with a view table. That view contains three columns, the entity, value, and timestamp. When a training set is registered, it is created as a materialized view via a JOIN on the corresponding label and feature views.

## Configuration <a href="#configuration" id="configuration"></a>

First we have to add a declarative Postgres configuration in Python.

{% code title="postgres_config.py" %}
```python
import featureform as ff

ff.register_postgres(
    name = "postgres_docs",
    description = "Example offline store store",
    team = "Featureform",
    host = "0.0.0.0",
    port = "5432",
    user = "postgres",
    password = "password",
    database = "postgres",
)
```
{% endcode %}

Once our config file is complete, we can apply it to our Featureform deployment

```bash
featureform apply postgres_config.py --host $FEATUREFORM_HOST
```

We can re-verify that the provider is created by checking the [Providers tab of the Feature Registry](../getting-started/exploring-the-feature-registry.md).

# Redshift

Featureform supports [Redshift](https://aws.amazon.com/redshift/) as an Offline Store.

## Implementation <a href="#implementation" id="implementation"></a>

### Primary Sources

#### Tables

Table sources are used directly via a view. Featureform will never write to a primary data set.

### Transformation Sources

SQL transformations are used to create a view. By default, those views are materialized and updated according to the schedule parameter. Deprecated transformations are converted to un-materialized views to save storage space.

### Offline to Inference Store Materialization

When a feature is registered, Featureform creates an internal transformation to get the newest value of every feature and its associated entity. A Kubernetes job is then kicked off to sync this up with the Inference store.

### Training Set Generation

Every registered feature and label is associated with a view table. That view contains three columns, the entity, value, and timestamp. When a training set is registered, it is created as a materialized view via a JOIN on the corresponding label and feature views.

## Configuration <a href="#configuration" id="configuration"></a>

First we have to add a declarative Redshift configuration in Python.

You will use the Postgres registration to set up a connection to the target Redshift instance.

{% code title="redshift_config.py" %}

```python
import featureform as ff

ff.register_postgres(
    name = "redshift_docs",
    description = "Example offline store store",
    team = "Featureform",
    host = "0.0.0.0",
    port = "5432",
    user = "redshift",
    password = "password",
    database = "redshift",
)

client.apply()
```

{% endcode %}

We can re-verify that the provider is created by checking the [Providers tab of the Feature Registry or via the CLI](../getting-started/search/monitor-discovery-feature-registry-ui-cli).

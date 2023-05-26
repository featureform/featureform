# Transforming Data

Featureform allows users to define transformations to create the tables they need for their features and training sets. Transformation definitions are versioned and stored immutably. They can be run on a schedule, as streaming transformations, or on demand. Featureform runs the transformations on your registered infrastructure providers.

## Registering Primary Data Sources

All features and labels originate from a set of initial data sources. These sources can be streams, files, or tables. A feature can simply be a single field of a data source. More commonly, it’s created via a set of transformations from one or multiple sources.

### Files

In the case of Offline Stores that work with a file system, the primary data may be a file or a set of files. Even when the Offline Store is a database, the primary data may be files that should be copied into a table in the Offline Store.

```python
s3.register_file(
    name = "titanic",
    variant = "kaggle",
    description = "The Titanic Dataset from Kaggle",
    path = "bucket/kaggle/titanic.csv",
)
```

### Tables

In the case of an Offline Store, the primary data may be a table that already exists. In this case, we can register the table with Featureform.

```python
titanic = postgres.register_table(
    name = "titanic",
    variant = "kaggle",
    description = "The Titanic Dataset from Kaggle",
    table = "Titanic", # This is the table's name in Postgres
)
```

### Streams

{% hint style="warning" %}
Streaming support coming soon in Enterprise Featureform. [Book a call with us today!](https://calendly.com/d/y5h-jpf-gj7/featureform-intro-call)
{% endhint %}

## Defining transformations

There are two supported transformation types: SQL and Dataframes. Not all providers support all transformation types. For example, a dataframe transformation cannot currently be run on Snowflake. Each transformation definition also includes a set of metadata like its name, variant, and description.

### SQL

SQL transformations are defined by a decorated python function that returns a templated SQL string. The decorator specifies the necessary metadata, and the SQL string has its table names replaced with templated source names and versions.

```python
@postgres.register_transformation(variant="quickstart")
def fare_per_family_member():
    """ The average fare paid per family member in a family
    """
    return "SELECT PassengerId, Fare / Parch FROM {{titanic.kaggle}}"
```

### Dataframes

Dataframe are defined by a decorated python function that takes a set of input dataframes and returns an output dataframe. The decorator specifies the necessary metadata and inputs, then the function outlines the transformation logic.

```python
@spark.register_transformation(
    variant="quickstart",
    inputs=[("titanic", "kaggle")],
)
def fare_per_family_member(titanic):
    """ The average fare paid per family member in a family
    """
    titanic["Fare/Parch"] = titanic["Fare"] / titanic["Parch"]
    return titanic[["PassengerId", "Fare/Parch"]]
```

### Chaining transformations

Transformations can be chained together, where one transformation is the input into another transformation. 
Transformations will wait for any dependent transformations to complete successfully before running.

```python
@postgres.register_transformation(variant="quickstart")
def survival_first_class():
    """ The age and survival status of first class passengers
    """
    return "SELECT age, survival FROM {{titanic.kaggle}} WHERE pclass='1st'"

@postgres.register_transformation(variant="quickstart")
def average_age_survival_first_class():
    """ The average survivability of first class passengers by age
    """
    return "SELECT age, AVG(survival) FROM {{survival_first_class.quickstart}} GROUP BY age"
```

## Checking status
Since some registrations may take longer than other depending on size of dataset and complexity of a query, the Featureform
API has the ability to check the status of any resource programatically. 

### Fetching the resource

```python
import featureform as ff
client = ff.ResourceClient(host)

source = client.get_source("name", "variant")
training_set = client.get_training_set("name", "variant")
label = client.get_label("name", "variant")
feature = client.get_feature("name", "variant")
```

### Checking the resource status
```python
import featureform as ff

training_set = client.get_training_set("name", "variant")
print(training_set.status == "READY")
print(training_set.get_status() == ff.ResourceStatus.READY)
print(training_set.is_ready())
```
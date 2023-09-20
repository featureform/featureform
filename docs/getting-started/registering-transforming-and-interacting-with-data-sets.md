# Transforming Data Sets

## Primary Data Sets

Once you've configured your data infrastructure providers, you can commence the process of registering your primary data sets. These primary data sets either directly contain your features and labels or serve as the foundation for their creation.

It's crucial to note that registering your primary data sets merely establishes a metadata reference to the data; it does NOT copy the data to Featureform. All data remains within your infrastructure and undergoes transformations there. Featureform takes on the role of an orchestrator.

There are three primary types of data sets that you can register: directories, files, and tables.

### Tables

Table-based Offline Stores, such as Snowflake, inherently revolve around tables. These providers furnish a method known as `.register_table(name, variant, table="")`. This method enables you to register transformations based on the primary data set or to register features and labels derived from it.

### Files

Offline Stores like Spark interact with file stores like S3 and HDFS. For these providers, there exists a method called `.register_file(name, variant="", path="")`.

Currently, Featureform offers support for CSVs and Parquet files. If your specific use case requires a different file format, please don't hesitate to raise an issue on [our Github repo](https://github.com/featureform/featureform).

## Transforming Data Sets

In most scenarios, primary data sets serve as the raw materials, which are then transformed into data sets containing the set of features and labels required for serving and training machine learning models. These transformations can be directly applied to primary data sets or sequenced and executed on other previously transformed data sets. It's essential to understand that, with the exception of pandas, Featureform itself doesn't perform the data transformations. Instead, it orchestrates your existing data infrastructure to execute the transformations.

Featureform boasts support for three languages for transformations: SQL, Dataframes, and Python. Python transformations are exclusively available for [on-demand transformations](../concepts/on-demand-features-request-time.md) and in [streaming transformations](../concepts/streaming.md).

### SQL Transformations

Featureform supports SQL transformations on providers like Snowflake, Spark, and Postgres, which natively support SQL. Given our orchestration approach that aligns with your data infrastructure, SQL transformations use the same SQL dialect as your provider. Our syntax incorporates a "mustache syntax" for referencing other registered Featureform data sets: `{{name.variant}}` or simply `{{name}}`.

To register a SQL transformation, utilize the `sql_transformation` method provided by an [offline store provider](../providers/offline-store.md). Decorate a Python function that returns a formatted SQL string. By default, the function name is used as the data set's name, and a [variant](../versioning-and-variants.md) is automatically generated. Both the name and variant can be overridden using kwargs of the same names in `sql_transformation`. Additionally, the function's docstring serves as the data set's description.

Example:

```python
@snowflake_provider.sql_transformation(variant="var")
def fn():
  """This transformation filters data where the value is greater than 10."""
  return "SELECT * from {{source.v4}} WHERE value > 10"

# We can retrieve any registered data set as a dataframe.
df = client.dataframe(fn)
```

### Dataframe Transformations

Featureform also offers support for Dataframe transformations, compatible with providers like Spark and Pandas on K8s that natively support Dataframes. The Dataframe object used is the native Dataframe object of the respective provider.

To register a Dataframe transformation, use the `df_transformation` method provided by an [offline store provider](../providers/offline-store.md) to decorate a Python function that returns a Dataframe. The `df_transformation` method requires a kwargs named `inputs`, which is a list of either `(name, variant)` tuples or Featureform data set objects. The function receives the Dataframe representation of these inputs as args. Similar to SQL transformations, the default name and variant are generated from the function's name, but these can be customized using kwargs within `df_transformation`. The function's docstring serves as the data set's description.

Example:

```python
@spark_provider.df_transformation(inputs=[("source", "v4")], variant="var")
def fn(src):
  """This transformation selects columns 'a', 'b', and 'c' from the 'source' dataset."""
  return src[["a", "b", "c"]]

# We can retrieve any registered data set as a dataframe.
df = client.dataframe(fn)
```

Featureform's transformation API empowers you to build the right features and labels tailored to your machine learning requirements using the syntax and logic you're used to, all while utilizing the strengths of your existing data infrastructure.

# Primary Data Sets

Once you've configured your data infrastructure, you can commence the process of registering your primary data sets. These primary data sets either directly contain your features and labels or serve as the foundation for their creation.

It's crucial to note that registering your primary data sets merely establishes a metadata reference to the data; it does NOT copy the data to Featureform. All data remains within your infrastructure and undergoes transformations there. Featureform takes on the role of an orchestrator.

There are three primary types of data sets that you can register: directories, files, and tables.

## Tables

Table-based Offline Stores, such as Snowflake, inherently revolve around tables. These providers furnish a method known as `.register_table(name, variant, table="")`. This method enables you to register transformations based on the primary data set or to register features and labels derived from it.

## Files

Offline Stores like Spark interact with file stores like S3 and HDFS. For these providers, there exists a method called `.register_file(name, variant="", path="")`.

Currently, Featureform offers support for CSVs and Parquet files. If your specific use case requires a different file format, please don't hesitate to raise an issue on Github or engage with our community on Slack. We value your feedback and are eager to explore new possibilities.

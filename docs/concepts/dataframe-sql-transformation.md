# Dataframe and SQL Transformation Support

Data scientists have diverse preferences in tools, with some favoring SQL while others lean towards Dataframes. Featureform transformations also exhibit varying compatibility with each API, often influenced by underlying data infrastructure like Postgres, which may support only one of the two.

A key aim of Featureform is to facilitate seamless adoption of your existing transformation logic. As a result, we offer robust support for both SQL-based and dataframe-based transformations. It's important to note that Featureform doesn't possess an intermediary transpiler between dataframes and SQL. Instead, our support is limited to the languages natively supported by your data infrastructure. For instance, direct application of dataframe transformations on Postgres isn't feasible.

Providers such as Pandas and Spark excel by offering native support for both SQL and dataframes. This flexibility allows you to employ and combine these approaches interchangeably.

Within Featureform, each provider features a *sql_transform* and/or *df_transform* method that yields a decorator:

```python
@spark.df_transformation(inputs=[("transactions", "quickstart")])
def average_user_transaction(transactions):
    """Calculate the average transaction amount per user"""
    return transactions.groupby("CustomerID")["TransactionAmount"].mean()

@spark.sql_transform()
def average_user_transaction():
   """Calculate the average transaction amount per user using SQL"""
   return "SELECT CustomerID, AVG(TransactionAmount) FROM {{transactions.quickstart}} GROUP BY CustomerID"
```

In this example, `spark` represents a pre-registered Featureform object. In the *df_transform* API, explicit input setting is necessary. The decorated function should return a dataframe. Conversely, the SQL API embeds inputs using the *{{name.variant}}* or *{{name}}* syntax, depending on variant availability. The function should return a SQL-like string. This versatility enables you to harness the full potential of both SQL and dataframe transformations, tailored to your specific requirements, preferences, and infrastructure.

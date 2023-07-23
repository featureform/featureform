# Metadata Tags

Featureform allows users to apply metadata tags to resources to logically group them together. Metadata tags can either be a list (`tags`), a set of key-value pairs (`properties`), or both, and are applicable to all resource types:

* Providers
* Sources
* Entities
* Features
* Labels
* Training Sets
* Models
* Users

## Example Config

In this example, we'll add tags and properties to all the resources included in the [Quickstart (Docker)](quickstart-docker.md):

{% code title="providers.py" %}

```python
import featureform as ff

postgres = ff.register_postgres(
    name="postgres-quickstart",
    host="host.docker.internal",
    port="5432",
    user="postgres",
    password="password",
    database="postgres",
    tags=["primary_training_data"],
    properties={"next_key_rotation": "2023-05-31"},
)

redis = ff.register_redis(
    name="redis-quickstart",
    host="host.docker.internal",
    port=6379,
    tags=["primary_inference_store"],
)

transactions = postgres.register_table(
    name="transactions",
    table="Transactions",
    tags=["pii_data"],
)


@postgres.sql_transformation(tags=["avg_aggregation"])
def average_user_transaction():
    return (
        "SELECT CustomerID as user_id, avg(TransactionAmount) "
        "as avg_transaction_amt from {{transactions.default}} GROUP BY user_id"
    )


@ff.entity
class User:
    # Register a column from our transformation as a feature
    avg_transactions = ff.Feature(
        average_user_transaction[["user_id", "avg_transaction_amt"]],
        variant: "quickstart",
        type=ff.Float32,
        inference_store=redis
    )
    # Register label from our base Transactions table
    fraudulent = ff.Label(
        transactions[["customerid", "isfraud"]],
        variant: "quickstart",
        type=ff.Bool,
    )

ff.register_training_set(
    "fraud_training", label="fraudulent", features=["avg_transactions"], tags=["transactions_v1", "pii_data"]
)
```

{% endcode %}

Then, we'll use the Featureform CLI to register these resources with the applied metadata tags.

```shell
featureform apply providers.py
```

## Updating Tags

Currently, we can update metadata tags that have already been applied to resources:

* `tags`: adding a new tag to a previously registered resource will append it to the list
* `properties`: adding a new key-value pair to a previously registered resource will add the pair to the set; if an pair contains an existing key, then the new value will overwrite the old value

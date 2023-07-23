# Scheduling Resource Updates

Often times we want to keep our Features and Training Sets up to date with the latest data. Featureform
offers the ability to schedule and run updates for Transformations, Features, Labels, and Training Sets.

Scheduling can be done during the registration of a resource or modified afterwards.

Updates are atomic. Once the resource has been created initially, all updates to the resource will be generated
in the background and will atomically replace the older version of the resource.

## Transformations

For transformations, the schedule can be specified in the decorator as a cron schedule.

```python
@postgres.sql_transformation(variant="quickstart", schedule="* * * * *")
def average_user_transaction():
    return "SELECT CustomerID as user_id, avg(TransactionAmount) " \
           "as avg_transaction_amt from {{transactions.kaggle}} GROUP BY user_id"
```

## Features and labels

For features and labels, the schedule can be specified as an argument. Everything registered in the call will be run
with the same schedule.

```python
@ff.entity
class User:
    # Register a column from our transformation as a feature
    avg_transactions = ff.Feature(
        average_user_transaction[["user_id", "avg_transaction_amt"]],
        variant: "quickstart",
        type=ff.Float32,
        inference_store=redis,
        schedule="* * * * *",
    )
    # Register label from our base Transactions table
    fraudulent = ff.Label(
        transactions[["customerid", "isfraud"]],
        variant: "quickstart",
        type=ff.Bool,
    )
```

## Training sets

For training sets, the schedule can be specified as an argument.

```python
ff.register_training_set(
    "fraud_training", "quickstart",
    label=("fraudulent", "quickstart"),
    features=[("avg_transactions", "quickstart")],
    schedule="* * * * *"
)
```

# Scheduling Resource Updates

Often times we want to keep our Features and Training Sets up to date with the latest data. Featureform
offers the ability to schedule and run updates for Transformations, Features, Labels, and Training Sets. 

Scheduling can be done during the registration of a resource or modified afterwards. 

Updates are atomic. Once the resource has been created initially, all updates to the resource will be generated
in the background and will atomically replace the older version of the resource.

### Transformations

For transformations, the schedule can be specified in the decorator as a cron schedule.

```python
@postgres.sql_transformation(variant="quickstart", schedule="* * * * *")
def average_user_transaction():
    return "SELECT CustomerID as user_id, avg(TransactionAmount) " \
           "as avg_transaction_amt from {{transactions.kaggle}} GROUP BY user_id"
```


### Features and labels

For features and labels, the schedule can be specified as an argument. Everything registered in the call will be run
with the same schedule.

```python
average_user_transaction.register_resources(
    entity=user,
    entity_column="user_id",
    inference_store=redis,
    features=[
        {"name": "avg_transactions", "variant": "quickstart", "column": "avg_transaction_amt", "type": "float32"},
    ],
    schedule="* * * * *"
)
```

### Training sets

For training sets, the schedule can be specified as an argument.

```python
ff.register_training_set(
    "fraud_training", "quickstart",
    label=("fraudulent", "quickstart"),
    features=[("avg_transactions", "quickstart")],
    schedule="* * * * *"
)
```

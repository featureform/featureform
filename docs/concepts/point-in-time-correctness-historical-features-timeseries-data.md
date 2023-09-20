# Achieving Point-in-Time Correctness and Handling Historical Features with Time Series Data

In the realm of time-series data, it's a common scenario for feature values to evolve over time. For instance, in a fraud detection model, you might define a feature like "user's average transaction amount" based on a series of transactions from your users. This value will continuously change as new transactions pour in. A typical training set comprises a label (what the model aims to predict) and a set of features. Each row often represents a historical transaction. Therefore, it's crucial for the feature values in these rows to reflect their state at the time of the associated label. This concept is known as "point-in-time correctness," where we need to obtain the historical values of features.

To illustrate point-in-time correctness, consider the creation of a training set using the following fraud label data:

| Transaction | User | Fraudulent Charge | Timestamp   |
| ----------- | ---- | ----------------- | ----------- |
| 1           | A    | False             | Jan 3, 2022 |
| 2           | A    | True              | Jan 5, 2022 |

Suppose we have a feature denoting the user's average purchase price:

| User | Avg Purchase Price | Timestamp   |
| ---- | ------------------ | ----------- |
| A    | 5                  | Jan 2, 2022 |
| A    | 10                 | Jan 4, 2022 |

The resulting training set should appear as follows:

| Avg Purchase Price | Fraudulent Charge |
| ------------------ | ----------------- |
| 5                  | False             |
| 10                 | True              |

Notice how the first row's feature value is 5, reflecting its state on Jan 3rd, 2022—the time of the first label. Conversely, the second row's feature value is 10, corresponding to the situation on Jan 5th, 2022—the time of the second label. This adherence to historical feature values ensures point-in-time correctness.

In Featureform, when defining features and labels, you have the option to include a timestamp column. By doing so, when creating the training set, Featureform will automatically align labels with features at the specified points in time.

Here's an example of defining features and labels with timestamps in Featureform:

```python
@ff.entity
class User:
    avg_transactions = ff.Feature(
        average_user_transaction[["CustomerID", "TransactionAmount"]],
        variant="quickstart",
        type=ff.Float32,
        inference_store=redis,
        timestamp_column="timestamp",
    )
    fraudulent = ff.Label(
        transactions[["CustomerID", "IsFraud"]],
        variant="quickstart",
        type=ff.Bool,
        timestamp_column="timestamp",
    )

ff.register_training_set(
    "fraud_training",
    "quickstart",
    label=("fraudulent", "quickstart"),
    features=[("avg_transactions", "quickstart")],
)

client.apply()

# The training set's feature values will be point-in-time correct.
ts = client.training_set("fraud_training", "quickstart").dataframe()
```

Additionally, it's worth noting that when working with the [streaming data API](streaming.md) in Featureform Enterprise, timestamps are tracked, and you have the capability to backfill data for point-in-time correct training sets as well. This comprehensive approach ensures the integrity of historical features in your machine learning workflows.

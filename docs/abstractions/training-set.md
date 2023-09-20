# Training Sets

Models require training, a process that typically involves feeding in a set of [features](feature.md) with known [labels](label.md). During training, the model makes inferences based on these features, and the labels are used to adjust the model's weights.

## Anatomy of a Training Set

A training set consists of a single label paired with one or more features. Below is an example of registering a training set named "fraud_training" with the [variant](../concepts/versioning-and-variants.md) "quickstart." It comprises the "fraudulent/quickstart" label and a single feature "avg_transactions/quickstart."

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

## How a Label gets joined With Features into a Training Set

Training sets are constructed by pairing [features](feature.md) and [labels](label.md) using their [entity](entity.md) key. The process involves looping through the labels and, for each feature, selecting the row with the same entity key as the label. To create [point-in-time correct training sets](../concepts/point-in-time-correctness-historical-features-timeseries-data.md), the feature value is obtained from the row with a timestamp closest to, but less than, the label timestamp. This ensures that the feature value aligns with the label's time.

## Working with Training Sets

### Using Dataframes

Every training set can be directly retrieved as a Dataframe. This approach is suitable for small datasets or when working with Dataframe-based training systems like Databricks. It offers ease and flexibility in handling training sets.

```python
ts = client.training_set("fraud_training", "quickstart").dataframe()
```

### Using Mini Batches

For larger training sets that do not fit into memory or when distributed Dataframes are not viable, you can iterate through them using mini-batches.

```python
import featureform as ff

client = ff.Client(host)
dataset = client.training_set(name, variant).repeat(5).shuffle(1000).batch(64)
for feature_batch, label_batch in dataset:
  # Run through a shuffled dataset 5 times in batches of 64
```

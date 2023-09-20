# Labels

**Labels** are a core component of a [training set](training-set.md). A model relies on a set of [features](feature.md) to make an inference. During the training process, this inference is compared to a label, and the model is adjusted incrementally.

## Defining a Label

### With a Timestamp

Some labels can change over time. For example, you might have a label indicating a user's most listened-to song in the last month. You define it in a manner similar to a [feature](feature.md) by specifying the dataset and columns for the entity, the value, and the timestamp. You can optionally set the variant and specify the type as one of: ff.Int, ff.Int32, ff.Int64, ff.Float32, ff.Float64, ff.Timestamp, ff.String, ff.Bool. Unlike a Feature, you should not specify an inference store since labels are never served for inference.

```python
import featureform as ff

@ff.entity
class User:
  top_song = ff.Label(dataset[["user", "top_song"]], variant="30days", type=ff.String)
```

### Without a Timestamp

Some labels are set once per entity and remain static. For example, you might have a label indicating whether a user is a bot or not. You define it in a manner similar to a [feature](feature.md) by specifying the dataset and columns for the entity and the value. You can optionally set the variant and specify the type as one of: ff.Int, ff.Int32, ff.Int64, ff.Float32, ff.Float64, ff.Timestamp, ff.String, ff.Bool. Unlike a Feature, you should not specify an inference store since labels are never served for inference.

```python
import featureform as ff

@ff.entity
class User:
  is_bot = ff.Label(dataset[["user", "is_bot"]], variant="simple", type=ff.Bool)
```

## Defining a Training Set

A [training set](training-set.md) comprises a label combined with a set of [features](feature.md). It can be defined as follows:

```python
ff.register_training_set(name, variant, label=(name, variant), features=[(name, variant)])
```

In cases where both the label and feature have training sets, we will automatically generate a [point-in-time correct training set](../concepts/point-in-time-correctness-historical-features-timeseries-data.md) for you.

You can then serve it as a dataframe or via a streaming method:

```python
client.training_set(name, variant).dataframe()
```

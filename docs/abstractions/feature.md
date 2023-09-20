# Features

**Features** represent the core abstraction in Featureform. They serve as inputs to machine learning models, providing context or observations that the model leverages to make inferences. In practice, feature engineering often yields the highest return on investment for data scientists, significantly improving model performance and reliability. Features are employed in two primary contexts: building training sets and serving for inference.

## The Anatomy of a Feature

A feature consists of a **value** and an associated [entity](entity) value. These features are defined based on registered data sets within Featureform. The entity effectively serves as a primary key or index for the feature values. While some features statically describe an entity (e.g., a restaurant's zip code), others change over time. When features exhibit temporal variations, it becomes crucial to employ point-in-time correct feature values in training sets to prevent data leakage and enhance model performance.

### Features without a Timestamp

Certain features remain relatively static. Examples include the category to which a product belongs. In such cases, the value of the feature is indexed by the entity, allowing you to look up the feature by entity.

To define a feature of this type, you add a Feature to an [entity](entity). The feature's first parameter specifies the Featureform dataset, indicating the entity column followed by the value column in the form `dataset[[entity_col, value_col]]`. Optionally, you can set the [variant](../concepts/versioning-and-variants). The type can take on one of the following values: `ff.Int`, `ff.Int32`, `ff.Int64`, `ff.Float32`, `ff.Float64`, `ff.Timestamp`, `ff.String`. Since features are typically served for inference to your trained model, it's essential to specify the [inference store](../providers/inference-store) for materializing the feature.

Example:
```python
import featureform as ff

@ff.entity
class User:
  age = ff.Feature(dataset[["user", "age"]], variant="simple", type=ff.Int, inference_store=redis)
```

### Features with a Timestamp

Some features exhibit changes over time, such as the highest priced item a user has purchased. In such cases, the feature's value is indexed by the entity, and you can access the feature's value as it existed at a specific timestamp. This is pivotal for creating [point-in-time correct training sets](../concepts/point-in-time-correctness-historical-features-timeseries-data).

To define a feature of this type, you add a Feature to an [entity](entity). The feature's first parameter specifies the Featureform dataset, encompassing the entity column, value column, and timestamp column in the form `dataset[[entity_col, value_col, timestamp_col]]`. Optionally, you can set the [variant](../concepts/versioning-and-variants). The type can take on one of the following values: `ff.Int`, `ff.Int32`, `ff.Int64`, `ff.Float32`, `ff.Float64`, `ff.Timestamp`, `ff.String`, `ff.Bool`. Since features are typically served for inference to your trained model, it's essential to specify the [inference store](../providers/inference-store) for materializing the feature. To maintain point-in-time correctness, only the most recent entity-feature pair is retained in the inference store.

Example:
```python
import featureform as ff

@ff.entity
class User:
  age = ff.Feature(dataset[["user", "top_item", "timestamp"]], variant="simple", type=ff.Int, inference_store=redis)
```

## Serving Features for Inference

Once a feature has been defined and applied, it will be materialized into the inference store for serving. The Featureform client provides a `features` method to serve your features.

Example:
```python
client.features([("age", "simple")], entities={"user": id})
```

This retrieves the most recent value of the feature for the specified entity.

## Building Training Sets with Features

A [training set](training-set) consists of a [label](label) joined with a set of features. You can define it as follows:

```python
ff.register_training_set(name, variant, label=(name, variant), features=[(name, variant)])
```

When both the label and feature have a timestamp, Featureform automatically generates [point-in-time correct training sets](../concepts/point-in-time-correctness-historical-features-timeseries-data) for you. You can then serve it as a dataframe or via a streaming method:

```python
client.training_set(name, variant).dataframe()
```

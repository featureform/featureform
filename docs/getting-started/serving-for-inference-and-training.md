# Serving for Inference and Training

Once features and training sets are registered, they can be served via Featureform's Python API.

## Serving for Inference

When a [feature is defined](defining-features-labels-and-training-sets.md#registering-features-and-labels), it is materialized into the [inference store](registering-infrastructure-providers.md#inference-store) associated with the definition. If a feature has a timestamp, only its most recent value is stored in the inference store. It can be thought of as a feature cache for inference time.

To serve a feature we initialize a serving client, specify the names and variants of the features we wish to serve, and provide the [entity](defining-features-labels-and-training-sets.md#registering-entities) whos feature values to look up. Underneath the hood, Featureform will keep a mapping between a feature's name and variant and the tables that they are actually stored in. This allows user's to stick to the Featureform level of abstraction.

```python
fpf = client.features([("fpf", "quickstart")], {"passenger": "1"})
```

## Serving for Training

When a [training set is defined](defining-features-labels-and-training-sets.md#registering-training-sets), it is materialized into the [offline store](registering-infrastructure-providers.md#offline-store) associated with the definition. The label from the training set is zipped with the features to form a [point-in-time correct](defining-features-labels-and-training-sets.md#point-in-time-correctness) dataset. Once that's complete we can initialize a ServingClient and loop through the our dataset

```python
import featureform as ff

client = ff.ServingClient(host)
dataset = client.dataset(name, variant)
for features, labels in dataset:
    # Train Model
```

### Dataset API

The Dataset API takes inspiration from Tensorflow's Dataset API, and both can be used very similarly.

#### Repeat

```python
import featureform as ff

client = ff.ServingClient(host)
for features, labels in client.dataset(name, variant).repeat(5):
    # Run through 5 epochs of the dataset
```

#### Shuffle

```python
import featureform as ff

client = ff.ServingClient(host)
buffer_size = 1000
for features, labels in client.dataset(name, variant).shuffle(buffer_size):
    # Run through a shuffled dataset
```

#### Batch

```python
import featureform as ff

client = ff.ServingClient(host)
for feature_batch, label_batch in client.dataset(name, variant).batch(8):
    # Run batches of features and labels
```

#### Putting it all together

The commands above can be chained together.

```python
import featureform as ff

client = ff.ServingClient(host)
dataset = client.dataset(name, variant).repeat(5).shuffle(1000).batch(64)
for feature_batch, label_batch in dataset:
    # Run through a shuffled dataset 5x in batches of 64
```

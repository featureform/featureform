# Using Embedding for Training

There are two common embedding use-cases in training. One is where the embeddings themselves are being trained and the other is when the embeddings are a constant input to a model that is being trained. This section will break down both cases.

## Using Constant Embeddings when Training
This is the simpler of the two patterns to train a model. In this case, we should create a snapshot of the spaces used for training to make sure they don't change during training.

Currently we only support local in-memory caching. In the future, remote caches can be used when a space’s size is too large to use in memory or when a snapshot needs to be maintained for a longer period of time for re-training.

```py
snapshot = hub.get_space("items", download_snapshot=True)
for key, label in training_samples:
	embedding = snapshot.get(key)
	model.training_step(embedding, label)
```

## Training Embeddings
In the case where the embeddings are actually mutable and being trained, the process looks slightly different. Local in-memory caching should be used when possible, and new embeddings can be written directly to the snapshot. At the end of training, the snapshot can be written back to the embedding hub.

```py
snapshot = hub.get_space("items", download_snapshot=True)
for key, label in training_samples:
	neighbors = snapshot.nearest_neighbor(5, key=key)
	model.training_step(neighbors, label)
	snapshot.set(key, model.embeddings[key])
snapshot.write_to_space("items_v2", create=True)
```

To avoid overwriting a space being used elsewhere, it’s typically preferable to write your snapshot under a new space name; however, you can choose to overwrite an existing space as well.

```py
snapshot.overwrite_space(“items”)
```

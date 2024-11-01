# Serving Embeddings for Inference

Using embeddings for inference is relatively simple in the most common case. The key-value and nearest-neighbor interfaces can be used to do whatever you need.

```py
def similar_items(item):
    return items_space.nearest_neighbor(10, key=item)

def user_recommendations(user):
	user_vec = user_space.get(user)
	return items_space.nearest_neighbor(10, vector=user_vec)
```

## Batching for increased throughput

The key-value and nearest neighbor interfaces both have support for performing multiple operations together. In use cases where a machine learning model is run on batches of data, this can dramatically improve throughput.

```py
def batch_user_recommendations(user_batch):
	user_vecs = user_space.multiget(user_batch)
	return items_space.multi_nearest_neighbor(10, vectors=user_vecs)
```

## Snapshots for minimal latency

Most Embeddinghub operations require a full round trip from client to server. Using batching, this can be pipelined, and throughput can be increased. However, if your system has very low latency requirements, a local snapshot can be used.

```py
user_snapshot = user_space.download_snapshot()
Item_snapshot = item_space.download_snapshot()

def user_recommendations(user):
	user_vec = user_snapshot.get(user)
	return item_snapshot.nearest_neighbor(10, vector=user_vec)
```

A snapshot refresh can be triggered either synchronously or asynchronously. This allows for fine-grained control on performance characteristics of the Embeddinghub.

```py
user_snapshot.refresh(wait=False)
```

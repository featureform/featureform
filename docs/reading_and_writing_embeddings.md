# Reading and Writing Embeddings

EmbeddingHub is made up of many user generated vector spaces. These spaces must be created and then they can be read and written to through one of the three following APIs:
* Key-Value
* Snapshot
* Nearest-Neighbor

## Spaces

Before writing and reading embeddings, we have to create a vector space to write and read from. A space has a name and a number of dimensions.

```py
space = hub.create_space("name",  3)
```

It can be retrieved by name as well.

```py
space = hub.get_space("name")
```

Once a space is created the following interfaces can be used to read and write to it.

## Key-Value Interface

The simplest native operations of the embedding store is writing and reading embeddings by key. This can be done one key at a time or more efficiently with multiple keys at once.

### Single Reads and Writes

Embeddings can be written and read one at a time

```py
space.set("key", [1, 2, 3])
embedding = space.get("key")
```

### Multi-Get and Multi-Set

When writing or reading many embeddings at once we provide a multi-get and multi-set interface. Both interfaces allow for more efficient retrieval and writing of embeddings.

```py
embeddings = {"a": [1, 2, 3], "b": [4, 5, 6]}
space.multiset(embeddings)
embeddings = space.multiget(["a", "b"])
```

Both interfaces also can work using generators for stream reading and writing. This is explained in the Bulk Loading section below.

### Deleting
Embeddings can also be deleted from a space. It can be done one at a time.

```py
space.delete("a")
```

Or with many at once

```py
space.multidelete(["a", "b", "c"])
```

A space can be completely cleared as well.

```py
space.delete_all()
```

### Bulk Loading

Multi-set can also take an iterator that allows for efficient bulk loading into a space. It expects an iterator of key-value pairs.

```py
Embeddings = [("a", [1, 2, 3]), ("b", [4, 5, 6]), ("c", [7, 8, 9])]
space.multiset(embeddings)
```

Multiget also works efficiently when provided with a generator or list. It actually stream-writes and reads from the embedding store. However, when doing a large read from or reading a space in full the Snapshot interface should be preferred.

## Snapshot Interface

A consistent snapshot can be created from a Space. The snapshot can also be written to and essentially treated as a fork of a space. There are many use cases of snapshots, a common one is to create a local in-memory snapshot of a space to be used in training.

```py
space.download_snapshot()
```

A local snapshot fulfils the nearest neighbor and key-value store interfaces to be dropped into code that expects a Space.

For brevity you can also create a snapshot when retrieving a space.

```py
space.get_space("name", download_snapshot=True)
```

## Nearest Neighbor Interface
A space allows for the nearest neighbors of an embedding to be retrieved. There are two ways to do so, via a key or via a vector. You can use a key if you want the nearest neighbors of an embedding that is stored in the space.

```py
space.nearest_neighbors(5, key=embedding_key)
```

You can use a vector when you want to find the nearest neighbors of an embedding that is not stored in the space.

```py
space.nearest_neigbhors(5, vector=embedding)
```

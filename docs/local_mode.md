# Using Embeddinghub in Local Mode

Embeddinghub can be used without a backend. If you're interested in running Embeddinghub locally via a Docker container backend check out our section on it, or head over to the [quickstart](/quickstart). If you're interested in using Embeddinghub as a better wrapper for embeddings locally, read on.

When connecting or creating an Embeddinghub instance, a LocalConfig can be used instead.

```py
import embeddinghub as eh

hub = eh.connect(LocalConfig("directory"))
```

This version of Embeddinghub works similarly to a traditional Embeddinghub instance in that you can create and get spaces and write and read from them.

```py
item_space = hub.create_space("items", 3)
```

One difference is that a local Embeddinghub should be saved to disk when done.

```py
hub.save()
```

This is done automatically when used with the `with` statement

```py
with hub as eh.connect(eh.LocalConfig("directory")):
	pass
```

All spaces and data are loaded into memory and used in-memory. An approximate nearest neighbor index is created using the same algorithm as the backend, so you can expect similar latency and throughput as you would with a local snapshot.

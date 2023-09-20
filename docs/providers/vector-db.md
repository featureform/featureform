# Vector Database

A Vector Database provider is designed to facilitate nearest neighbor lookups. It shares several similarities with an inference store but is distinguished by its support for the `client.nearest` API. Configuration is typically done when registering an [embedding](../abstractions/embedding.md) associated with an [entity](../abstractions/entity.md). This setup enables efficient retrieval of nearest neighbors based on feature vectors.

```python
client.nearest([(name, variant)], vec, numResults)
```

With the Vector Database provider, you can perform nearest neighbor queries to find similar entities or data points based on their feature vectors. This functionality is particularly valuable in various use cases, such as recommendation systems, content similarity analysis, and LLM applications.

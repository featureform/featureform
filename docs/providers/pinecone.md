# Pinecone

Featureform supports [Pinecone](https://pinecone.io/) as a [Vector DB](vector-db.md).

## Configuration

First we have to add a declarative Pinecone configuration in Python. The following example shows the required fields.

```python
import featureform as ff

pinecone = ff.register_pinecone(
    name="pinecone-quickstart",
    project_id="2g13ek7",
    environment="us-west4-gcp-free",
    api_key="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
)

client.apply()
```

Once our config file is complete, we can apply it to our Featureform deployment. Afterwards we can set it as the [Vector DB](vector-db.md) when defining an [embedding](../abstractions/embedding.md).

We can re-verify that the provider is created by checking the [Providers tab of the Feature Registry or via the CLI](../getting-started/search/monitor-discovery-feature-registry-ui-cli.md).

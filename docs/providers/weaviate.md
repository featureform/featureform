# Pinecone

Featureform supports [Weaviate](https://weaviate.io/) as a [Vector DB](vector-db).

## Configuration

First we have to add a declarative Pinecone configuration in Python. The following example shows the required fields.

```python
import featureform as ff

weaviate = ff.register_weaviate(
    name="weaviate-quickstart",
    url="https://<CLUSTER NAME>.weaviate.network",
    api_key="<API KEY>"
    description="A Weaviate project for using embeddings in Featureform"
)

client.apply()
```

Once our config file is complete, we can apply it to our Featureform deployment. Afterwards we can set it as the [Vector DB](vector-db) when defining an [embedding](../abstractions/embedding).

We can re-verify that the provider is created by checking the [Providers tab of the Feature Registry or via the CLI](../getting-started/search/monitor-discovery-feature-registry-ui-cli).

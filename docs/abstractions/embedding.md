# Embeddings

**Embeddings** represent a specialized type of [feature](feature.md) stored in a [Vector DB](../providers/vector-db.md). They are primarily used for nearest neighbor lookups. If you'd like to explore a comprehensive explanation of what an embedding is, you can read our [definitive guide to embeddings](https://www.featureform.com/post/the-definitive-guide-to-embeddings).

Defining an embedding is quite similar to [defining a feature](feature.md). It's associated with an [entity](entity.md), and the data is stored in a data set. An embedding must be a vector of floats, so all that needs specification is the dimensions of the embedding and the vector DB in which it's stored.

Here's an example:
```python
comment_embeddings = ff.Embedding(
    vectorize_comments[["PK", "Vector"]],
    dims=384,
    vector_db=pinecone,
    description="Embeddings created from speakers' comments in episodes",
    variant="v1"
)
```

Additionally, we provide a more detailed [end-to-end example of building with embeddings](../embeddings/building-a-chatbot-with-openai-and-a-vector-database.md).

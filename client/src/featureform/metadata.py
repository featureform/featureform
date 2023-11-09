from .sqlite_metadata import SQLiteMetadata
from .resources import Provider, EmptyConfig, PineconeConfig, WeaviateConfig


def get_provider_config(provider_type):
    ptype = {
        "LOCAL_ONLINE": EmptyConfig,
        "PINECONE_ONLINE": PineconeConfig,
        "WEAVIATE_ONLINE": WeaviateConfig,
        "": EmptyConfig,
    }
    return ptype[provider_type]()


def get_provider(name):
    sql = SQLiteMetadata()
    row = sql.get_provider(name, False)
    config = get_provider_config(row["provider_type"]).deserialize(
        row["serialized_config"]
    )

    provider = Provider(
        name=row["name"],
        description=row["description"],
        team=row["team"],
        config=config,
        function=row["provider_type"],
        status=row["status"],
    )
    sql.close()
    return provider

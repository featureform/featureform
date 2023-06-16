from .filestore import LocalFileStore
from .pinecone import PineconeOnlineStore
from .weaviate import WeaviateOnlineStore
from .online_store import OnlineStore


def get_provider(provider) -> OnlineStore:
    providers = {
        "LOCAL_ONLINE": LocalFileStore,
        "PINECONE_ONLINE": PineconeOnlineStore,
        "WEAVIATE_ONLINE": WeaviateOnlineStore,
    }
    if provider in providers:
        return providers[provider]
    else:
        raise NotImplementedError(f"Provider {provider} not implemented")

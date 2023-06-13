from .filestore import LocalFileStore
from .online_store import OnlineStore


def get_provider(provider) -> OnlineStore:
    providers = {
        "file": LocalFileStore,
    }
    if provider in providers:
        return providers[provider]()
    else:
        raise NotImplementedError(f"Provider {provider} not implemented")

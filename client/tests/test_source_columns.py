import featureform as ff
import inspect
from typing import Union, Optional
from featureform.register import (
    SourceRegistrar,
    SubscriptableTransformation,
)


def test_client_has_columns_method():
    client = ff.Client(host="localhost:7878", insecure=True)
    assert hasattr(client, "columns")


def test_columns_method_signature():
    sig = inspect.signature(ff.Client.columns)

    params = sig.parameters

    assert (
        params["source"].annotation
        == Union[SourceRegistrar, SubscriptableTransformation, str]
    )
    assert params["variant"].annotation == Optional[str]
    assert params["variant"].default == None

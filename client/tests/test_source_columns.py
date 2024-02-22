import featureform as ff
import inspect
from typing import Union, Optional
from featureform.register import (
    SourceRegistrar,
    SubscriptableTransformation,
)


def test_client_has_columns_method():
    client = ff.Client()
    assert hasattr(client, "columns")


def test_columns_method_signature():
    sig = inspect.signature(ff.Client.columns)

    params = sig.parameters

    for name, param in params.items():
        if name == "self":
            continue
        if name == "source":
            assert (
                param.annotation
                == Union[SourceRegistrar, SubscriptableTransformation, str]
            )
        if name == "variant":
            assert param.annotation == Optional[str]
            assert param.default == None

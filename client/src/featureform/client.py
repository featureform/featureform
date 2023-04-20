from typing import Union
import warnings
from .register import (
    ResourceClient,
    SourceRegistrar,
    LocalSource,
    SubscriptableTransformation,
)
from .serving import ServingClient
from .enums import ApplicationMode
import pandas as pd


class Client(ResourceClient, ServingClient):
    """
    Client for interacting with Featureform APIs (resources and serving)

    **Using the Client:**
    ```py title="definitions.py"
    import featureform as ff
    from featureform import Client

    client = Client("http://localhost:8080")

    # Example 1: Get a registered provider
    redis = client.get_provider("redis-quickstart")

    # Example 2: Compute a dataframe from a registered source
    transactions_df = client.compute_df("transactions", "quickstart")
    """

    def __init__(
        self, host=None, local=False, insecure=False, cert_path=None, dry_run=False
    ):
        ResourceClient.__init__(
            self,
            host=host,
            local=local,
            insecure=insecure,
            cert_path=cert_path,
            dry_run=dry_run,
        )
        # Given both ResourceClient and ServingClient are instantiated together, if dry_run is True, then
        # the ServingClient cannot be instantiated due to a conflict the local and host arguments.
        if not dry_run:
            ServingClient.__init__(
                self, host=host, local=local, insecure=insecure, cert_path=cert_path
            )
            self.application_mode = (
                ApplicationMode.LOCAL if local else ApplicationMode.HOSTED
            )

    def compute_df(
        self,
        source: Union[SourceRegistrar, LocalSource, SubscriptableTransformation, str],
        variant="default",
    ):
        """
        Compute a dataframe from a registered source or transformation

        Args:
            source (Union[SourceRegistrar, LocalSource, SubscriptableTransformation, str]): The source or transformation to compute the dataframe from
            variant (str): The source variant; defaults to "default" and is ignored if source argument is not a string

        **Example:**
        ```py title="definitions.py"
        transactions_df = client.compute_df("transactions", "quickstart")

        avg_user_transaction_df = transactions_df.groupby("CustomerID")["TransactionAmount"].mean()
        """
        if isinstance(
            source, (SourceRegistrar, LocalSource, SubscriptableTransformation)
        ):
            name, variant = source.name_variant()
        elif isinstance(source, str):
            name = source
        else:
            raise ValueError(
                f"source must be of type SourceRegistrar, LocalSource, SubscriptableTransformation or str, not {type(source)}"
            )
        if self.application_mode == ApplicationMode.LOCAL:
            return self.impl.get_input_df(name, variant)
        elif self.application_mode == ApplicationMode.HOSTED:
            warnings.warn(
                "Computing dataframes on sources in hosted mode is not yet supported."
            )
            return pd.DataFrame()
        else:
            raise ValueError(f"ApplicationMode {ApplicationMode} not supported.")

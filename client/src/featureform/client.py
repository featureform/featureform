from typing import Union
from .register import (
    ResourceClient,
    SourceRegistrar,
    LocalSource,
    SubscriptableTransformation,
    FeatureColumnResource,
)
from .serving import ServingClient
from .constants import NO_RECORD_LIMIT
from .names_generator import get_random_name


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
    transactions_df = client.dataframe("transactions", "quickstart")
    ```
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

    def dataframe(
        self,
        source: Union[SourceRegistrar, LocalSource, SubscriptableTransformation, str],
        variant: Union[str, None] = None,
        limit=NO_RECORD_LIMIT,
    ):
        """
        Compute a dataframe from a registered source or transformation

        Args:
            source (Union[SourceRegistrar, LocalSource, SubscriptableTransformation, str]): The source or transformation to compute the dataframe from
            variant (str): The source variant; defaults to a Docker-style random name and is ignored if source argument is not a string
            limit (int): The maximum number of records to return; defaults to NO_RECORD_LIMIT

        **Example:**
        ```py title="definitions.py"
        transactions_df = client.dataframe("transactions", "quickstart")

        avg_user_transaction_df = transactions_df.groupby("CustomerID")["TransactionAmount"].mean()
        ```
        """
        self.apply()
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
        variant = get_random_name() if variant is None else variant
        return self.impl._get_source_as_df(name, variant, limit)

    def nearest(self, feature, vector, k):
        """
        Query the K nearest neighbors of a provider vector in the index of a registered feature variant

        Args:
            name (str): Feature name
            variant (str): Feature variant
            vector (List[float]): Query vector
            k (int): Number of nearest neighbors to return

        **Example:**
        ```py title="definitions.py"
        # Get the 5 nearest neighbors of the vector [0.1, 0.2, 0.3] in the index of the feature "my_feature" with variant "my_variant"
        nearest_neighbors = client.nearest("my_feature", "my_variant", [0.1, 0.2, 0.3], 5)
        print(nearest_neighbors) # prints a list of entities (e.g. ["entity1", "entity2", "entity3", "entity4", "entity5"])
        ```
        """
        if isinstance(feature, tuple):
            name, variant = feature
        elif isinstance(feature, FeatureColumnResource):
            name = feature.name
            variant = feature.variant
        else:
            raise Exception(
                f"the feature '{feature}' of type '{type(feature)}' is not support."
                "Feature must be a tuple of (name, variant) or a FeatureColumnResource"
            )

        if k < 1:
            raise ValueError(f"k must be a positive integer")
        return self.impl.nearest(name, variant, vector, k)

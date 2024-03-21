import featureform.resources
from typing import Union, Optional

from .constants import NO_RECORD_LIMIT
from .register import (
    ResourceClient,
    SourceRegistrar,
    SubscriptableTransformation,
    FeatureColumnResource,
    TriggerResource,
    TrainingSetVariant,
    LabelColumnResource,
)
from .resources import (
    ScheduleTriggerResource,
    TriggerResource,
)
from .serving import ServingClient
from .enums import ResourceType
from featureform.proto import metadata_pb2


class Client(ResourceClient, ServingClient):
    """
    Client for interacting with Featureform APIs (resources and serving)

    **Using the Client:**
    ```py title="definitions.py"
    import featureform as ff
    from featureform import Client
    from featureform.client.src.featureform import metadata_pb2

    client = Client()

    # Example 1: Get a registered provider
    redis = client.get_provider("redis-quickstart")

    # Example 2: Compute a dataframe from a registered source
    transactions_df = client.dataframe("transactions", "quickstart")
    ```
    """

    def __init__(
        self, host=None, local=False, insecure=False, cert_path=None, dry_run=False
    ):
        if local:
            raise Exception(
                "Local mode is not supported in this version. Use featureform <= 1.12.0 for localmode"
            )

        if host is not None:
            self._validate_host(host)

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
        source: Union[SourceRegistrar, SubscriptableTransformation, str],
        variant: Optional[str] = None,
        limit=NO_RECORD_LIMIT,
        asynchronous=False,
        verbose=False,
    ):
        """
        Return a dataframe from a registered source or transformation

        **Example:**
        ```py title="definitions.py"
        transactions_df = client.dataframe("transactions", "quickstart")

        avg_user_transaction_df = transactions_df.groupby("CustomerID")["TransactionAmount"].mean()
        ```

        Args:
            source (Union[SourceRegistrar, SubscriptableTransformation, str]): The source or transformation to compute the dataframe from
            variant (str): The source variant; can't be None if source is a string
            limit (int): The maximum number of records to return; defaults to NO_RECORD_LIMIT
            asynchronous (bool): Flag to determine whether the client should wait for resources to be in either a READY or FAILED state before returning. Defaults to False to ensure that newly registered resources are in a READY state prior to serving them as dataframes.

        Returns:
            df (pandas.DataFrame): The dataframe computed from the source or transformation

        """
        self.apply(asynchronous=asynchronous, verbose=verbose)
        if isinstance(source, (SourceRegistrar, SubscriptableTransformation)):
            name, variant = source.name_variant()
        elif isinstance(source, str):
            name = source
            if variant is None:
                raise ValueError("variant must be specified if source is a string")
            if variant == "":
                raise ValueError("variant cannot be an empty string")
        else:
            raise ValueError(
                f"source must be of type SourceRegistrar, SubscriptableTransformation or str, not {type(source)}\n"
                "use client.dataframe(name, variant) or client.dataframe(source) or client.dataframe(transformation)"
            )
        return self.impl._get_source_as_df(name, variant, limit)

    def nearest(self, feature, vector, k):
        """
        Query the K nearest neighbors of a provider vector in the index of a registered feature variant

        **Example:**

        ```py title="definitions.py"
        # Get the 5 nearest neighbors of the vector [0.1, 0.2, 0.3] in the index of the feature "my_feature" with variant "my_variant"
        nearest_neighbors = client.nearest("my_feature", "my_variant", [0.1, 0.2, 0.3], 5)
        print(nearest_neighbors) # prints a list of entities (e.g. ["entity1", "entity2", "entity3", "entity4", "entity5"])
        ```

        Args:
            feature (Union[FeatureColumnResource, tuple(str, str)]): Feature object or tuple of Feature name and variant
            vector (List[float]): Query vector
            k (int): Number of nearest neighbors to return

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
            raise ValueError("k must be a positive integer")
        return self.impl.nearest(name, variant, vector, k)

    def location(
        self,
        source: Union[SourceRegistrar, SubscriptableTransformation, str],
        variant: Optional[str] = None,
        resource_type: Optional[ResourceType] = None,
    ):
        """
        Returns the location of a registered resource. For SQL resources, it will return the table name
        and for file resources, it will return the file path.

        **Example:**
        ```py title="definitions.py"
        transaction_location = client.location("transactions", "quickstart", ff.SOURCE)
        ```

        Args:
            source (Union[SourceRegistrar, SubscriptableTransformation, str]): The source or transformation to compute the dataframe from
            variant (str): The source variant; can't be None if source is a string
            resource_type (ResourceType): The type of resource; can be one of ff.SOURCE, ff.FEATURE, ff.LABEL, or ff.TRAINING_SET
        """
        if isinstance(source, (SourceRegistrar, SubscriptableTransformation)):
            name, variant = source.name_variant()
            resource_type = ResourceType.SOURCE
        elif isinstance(source, featureform.resources.TrainingSetVariant):
            name = source.name
            variant = source.variant
            resource_type = ResourceType.TRAINING_SET
        elif isinstance(source, str):
            name = source
            if variant is None:
                raise ValueError("variant must be specified if source is a string")
            if variant == "":
                raise ValueError("variant cannot be an empty string")

            if resource_type is None:
                raise ValueError(
                    "resource_type must be specified if source is a string"
                )

        else:
            raise ValueError(
                f"source must be of type SourceRegistrar, SubscriptableTransformation or str, not {type(resource)}\n"
                "use client.dataframe(name, variant) or client.dataframe(source) or client.dataframe(transformation)"
            )

        location = self.impl.location(name, variant, resource_type)
        return location

    def close(self):
        """
        Closes the client, closes channel for hosted mode
        """
        self.impl.close()

    def columns(
        self,
        source: Union[SourceRegistrar, SubscriptableTransformation, str],
        variant: Optional[str] = None,
    ):
        """
        Returns the columns of a registered source or transformation

        **Example:**
        ```py title="definitions.py"
        columns = client.columns("transactions", "quickstart")
        ```

        Args:
            source (Union[SourceRegistrar, SubscriptableTransformation, str]): The source or transformation to get the columns from
            variant (str): The source variant; can't be None if source is a string

        Returns:
            columns (List[str]): The columns of the source or transformation
        """
        if isinstance(source, (SourceRegistrar, SubscriptableTransformation)):
            name, variant = source.name_variant()
        elif isinstance(source, str):
            name = source
            if variant is None:
                raise ValueError("variant must be specified if source is a string")
            if variant == "":
                raise ValueError("variant cannot be an empty string")
        else:
            raise ValueError(
                f"source must be of type SourceRegistrar, SubscriptableTransformation or str, not {type(source)}\n"
                "use client.columns(name, variant) or client.columns(source) or client.columns(transformation)"
            )
        return self.impl._get_source_columns(name, variant)

    def _create_trigger_proto(self, trigger):
        req = metadata_pb2.Trigger()
        if isinstance(trigger, str):
            req.name = trigger
        elif isinstance(trigger, TriggerResource):
            req.name = trigger.name
        else:
            raise ValueError(
                f"Invalid trigger type: {type(trigger)}. Please use the trigger name or TriggerResource"
            )

        return req

    def _create_resource_proto(self, resource):
        req = metadata_pb2.ResourceID()
        if isinstance(resource, tuple):
            if len(resource) != 3:
                raise ValueError(
                    f"Invalid resource tuple: {resource}. resource tuple must have name, variant and type."
                )
            req.resource.name = resource[0]
            req.resource.variant = resource[1]
            req.resource_type = resource[2]
        else:
            if isinstance(resource, FeatureColumnResource):
                req.resource_type = ResourceType.FEATURE.value
            elif isinstance(resource, TrainingSetVariant):
                req.resource_type = ResourceType.TRAINING_SET.value
            elif isinstance(resource, SourceRegistrar):
                req.resource_type = ResourceType.SOURCE.value
            elif isinstance(resource, LabelColumnResource):
                req.resource_type = ResourceType.LABEL.value

            else:
                raise ValueError(
                    f"Invalid resource type: {type(resource)}. Resource must be a Feature, Training Set or Source."
                )
            req.resource.name = resource.name
            req.resource.variant = resource.variant
        return req

    def add_trigger(self, trigger, resource):
        """
        Add a trigger to a resource

        **Example:**
        ```py title="definitions.py"
        client.add_trigger("trigger_name", ("resource_name", "resource_variant, resource_type"))
        ```

        Args:
            trigger(Union[str, TriggerResource]): The name of the trigger
            resource(Union[tuple, FeatureColumnResource, TrainingSetVariant]): The name, variant and type of the resource
        """

        req = metadata_pb2.AddTriggerRequest()
        trigger_req = self._create_trigger_proto(trigger)
        req.trigger.CopyFrom(trigger_req)
        resource_req = self._create_resource_proto(resource)
        print("In add_trigger resource type is ", resource_req.resource_type)
        req.resource.CopyFrom(resource_req)

        self._stub.AddTrigger(req)

    def remove_trigger(self, trigger, resource):
        """
        Remove a trigger from a resource

        **Example:**
        ```py title="definitions.py"
        client.remove_trigger("trigger_name", ("resource_name", "resource_variant, resource_type"))
        ```

        Args:
            trigger(Union[str, TriggerResource]): The name of the trigger
            resource(Union[tuple, FeatureColumnResource, TrainingSetVariant]): The name, variant and type of the resource
        """
        req = metadata_pb2.RemoveTriggerRequest()
        trigger_req = self._create_trigger_proto(trigger)
        req.trigger.CopyFrom(trigger_req)
        resource_req = self._create_resource_proto(resource)
        req.resource.CopyFrom(resource_req)

        self._stub.RemoveTrigger(req)

    def update_trigger(self, trigger, schedule):
        """
        Update the schedule of the trigger

        **Example:**
        ```py title="definitions.py"
        client.update_trigger("trigger_name", schedule)
        ```

        Args:
            trigger_name (Union[str, TriggerResource]): The name of the trigger
            TODO: schedule (str): The new schedule for the trigger
        """
        if not isinstance(trigger, ScheduleTriggerResource):
            raise ValueError(
                f"Invalid schedule type: {type(schedule)}. Please use the ScheduleTrigger object."
            )
        if not isinstance(schedule, str):
            raise ValueError(
                f"Invalid schedule type: {type(schedule)}. Please use the string format for the schedule."
            )
        trigger.update_schedule(schedule)
        req = self._create_trigger_proto(trigger)
        schedule_req = metadata_pb2.ScheduleTrigger()
        schedule_req.schedule = schedule
        req.schedule_trigger.CopyFrom(schedule_req)

        self._stub.CreateTrigger(req)

    def delete_trigger(self, trigger):
        """
        Delete a trigger from the storage provider

        **Example:**
        ```py title="definitions.py"
        client.delete_trigger("trigger_name")
        ```

        Args:
            trigger_name (Union[str, TriggerResource]): The name of the trigger
        """
        req = self._create_trigger_proto(trigger)
        self._stub.DeleteTrigger(req)

    def get_trigger(self, trigger):
        """
        Get a trigger from the storage provider

        **Example:**
        ```py title="definitions.py"
        client.get_trigger("trigger_name")
        ```

        Args:
            trigger_name (str): The name of the trigger
        """
        req = self._create_trigger_proto(trigger)
        return self._stub.GetTrigger(req)

    @staticmethod
    def _validate_host(host):
        if host.startswith("http://") or host.startswith("https://"):
            raise ValueError("Invalid Host: Host should not contain http or https.")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

from typing import List, Any, Union
import uuid
import weaviate
import numpy as np

from .online_store import VectorStoreTable, VectorStore, ValueType, VectorType
from ..resources import WeaviateConfig
from ..enums import ScalarType


class WeaviateOnlineTable(VectorStoreTable):
    def __init__(self, client, class_name: str, vector_type: ValueType):
        self.client = client
        self.class_name = class_name
        self.vector_type = vector_type

    def set(self, entity: str, value: Union[List[float], np.ndarray]):
        id = self._create_id(entity)
        properties = {"entity": entity}
        if self.client.data_object.exists(uuid=id, class_name=self.class_name):
            self.client.data_object.replace(
                data_object=properties,
                class_name=self.class_name,
                uuid=id,
                vector=value,
            )
        else:
            self.client.data_object.create(
                data_object=properties,
                class_name=self.class_name,
                uuid=id,
                vector=value,
            )

    def get(self, entity: str):
        id = self._create_id(entity)
        resp = self.client.data_object.get_by_id(
            uuid=id, class_name=self.class_name, with_vector=True
        )
        return resp.get("vector")

    def nearest(
        self, feature: str, variant: str, vector: Union[List[float], np.ndarray], k: int
    ):
        if vector is np.ndarray:
            vector = vector.tolist()
        resp = (
            self.client.query.get(class_name=self.class_name, properties=["entity"])
            .with_near_vector(
                {
                    "vector": vector,
                }
            )
            .with_limit(k)
            .do()
        )
        matches = resp["data"]["Get"][self.class_name]
        return [m["entity"] for m in matches]

    def _create_id(self, entity: str) -> str:
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, entity))


class WeaviateOnlineStore(VectorStore):
    def __init__(self, config: WeaviateConfig):
        self.class_name_template = "Featureform_class__{0}"
        self.client = weaviate.Client(
            url=config.url,
            auth_client_secret=weaviate.AuthApiKey(api_key=config.api_key),
        )

    def get_table(self, feature: str, variant: str):
        class_name = self._create_class_name(feature, variant)
        return WeaviateOnlineTable(
            client=self.client,
            class_name=class_name,
            vector_type=VectorType(
                scalar_type=ScalarType.FLOAT32,
                # Weaviate indexes (i.e. schema + classes) don't actually need the dimension upon
                # creation, and there doesn't appear to be a place to fetch the dimension from
                # the schema/class once it's been created; technically, in the context of localmode,
                # we don't need the dimension either; however, if it becomes useful, we'll need to
                # this and determine how to handle this. One option is to store the dimension
                # as a property of the class, but that would put the property field on every record
                # in the index.
                dimension=0,
                is_embedding=True,
            ),
        )

    def create_table(self, feature: str, variant: str, value_type: ValueType):
        return WeaviateOnlineTable(
            client=self.client,
            class_name=self._create_class_name(feature, variant),
            vector_type=value_type,
        )

    def delete_table(self, feature: str, variant: str):
        pass

    def table_exists(self, feature: str, variant: str) -> bool:
        class_name = self._create_class_name(feature, variant)
        return self.client.schema.exists(class_name)

    def close(self):
        pass

    def create_index(self, feature: str, variant: str, vector_type: VectorType):
        class_name = self._create_class_name(feature, variant)
        self.client.schema.create_class(
            {
                "class": class_name,
                "properties": [
                    {
                        "name": "entity",
                        "description": "The entity value (i.e. name/ID) of the feature variant",
                        "dataType": ["text"],
                        "indexInverted": False,
                    },
                ],
            }
        )
        return WeaviateOnlineTable(
            client=self.client,
            class_name=class_name,
            vector_type=vector_type,
        )

    def delete_index(self, feature: str, variant: str):
        class_name = self._create_class_name(feature, variant)
        self.client.schema.delete_class(class_name)

    def _create_class_name(self, feature: str, variant: str) -> str:
        # Class names must adhere to the following regex: /^[A-Z][_0-9A-Za-z]*$/
        # To ensure uniqueness and keep things simple for the user,
        # we created a deterministic UUID5 based on the feature and variant
        # and replace all "-" with "_" to ensure it adheres to the regex
        # See details: https://weaviate.io/developers/weaviate/config-refs/schema#class
        index_uuid = uuid.uuid5(uuid.NAMESPACE_DNS, f"{feature}-{variant}")
        index_uuid = str(index_uuid).replace("-", "_")
        return self.class_name_template.format(index_uuid)

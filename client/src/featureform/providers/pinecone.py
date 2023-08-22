from typing import List, Any, Union
import uuid
import pinecone
import numpy as np

from .online_store import VectorStoreTable, VectorStore, ValueType, VectorType
from ..resources import PineconeConfig
from ..enums import ScalarType


class PineconeOnlineTable(VectorStoreTable):
    def __init__(self, client, index_name: str, namespace: str, vector_type: ValueType):
        self.client = client
        self.index_name = index_name
        self.namespace = namespace
        self.vector_type = vector_type

    def set(self, entity: str, value: Union[List[float], np.ndarray]):
        self.client.Index(self.index_name).upsert(
            vectors=[(self._create_id(entity), value, {"id": entity})],
            namespace=self.namespace,
        )

    def get(self, entity: str):
        id = self._create_id(entity)
        fetch_response = self.client.Index(self.index_name).fetch(
            ids=[id],
            namespace=self.namespace,
        )
        vector = fetch_response["vectors"].get(id)
        if vector is None:
            return None
        else:
            return vector["values"]

    def nearest(
        self, feature: str, variant: str, vector: Union[List[float], np.ndarray], k: int
    ):
        name_variant_id = uuid.uuid5(uuid.NAMESPACE_DNS, f"{feature}-{variant}")
        if isinstance(vector, np.ndarray):
            vector = vector.tolist()
        query_response = self.client.Index(f"ff-idx--{name_variant_id}").query(
            namespace=self.namespace, vector=vector, top_k=k, include_metadata=True
        )
        matches = query_response.get("matches", [])
        if len(matches) == 0:
            return []
        else:
            return [match["metadata"]["id"] for match in matches]

    def _create_id(self, entity: str) -> str:
        id_uuid = uuid.uuid5(uuid.NAMESPACE_DNS, entity)
        return str(id_uuid)


class PineconeOnlineStore(VectorStore):
    def __init__(self, config: PineconeConfig):
        self.index_name_template = "ff-idx--{0}"
        pinecone.init(api_key=config.api_key, environment=config.environment)
        self.client = pinecone

    def get_table(self, feature: str, variant: str) -> PineconeOnlineTable:
        idx = self._create_index_name(feature, variant)
        describe_response = self.client.Index(idx).describe_index_stats()
        return PineconeOnlineTable(
            client=self.client,
            index_name=idx,
            namespace=self._create_namespace(feature, variant),
            vector_type=VectorType(
                scalar_type=ScalarType.FLOAT32,
                dimension=describe_response.dimension,
                is_embedding=True,
            ),
        )

    def create_table(self, feature: str, variant: str, entity_type: ValueType):
        return PineconeOnlineTable(
            client=self.client,
            index_name=self._create_index_name(feature, variant),
            namespace=self._create_namespace(feature, variant),
            vector_type=entity_type,
        )

    def table_exists(self, feature: str, variant: str) -> bool:
        try:
            self.client.describe_index(self._create_index_name(feature, variant))
            return True
        except pinecone.core.client.exceptions.NotFoundException:
            return False

    def delete_index(self, feature: str, variant: str):
        index_name = self._create_index_name(feature, variant)
        self.client.delete_index(name=index_name)

    def delete_table(self, feature: str, variant: str):
        pass

    def close(self):
        pass

    def create_index(self, feature: str, variant: str, vector_type: VectorType):
        index_name = self._create_index_name(feature, variant)
        self.client.create_index(
            name=index_name, dimension=vector_type.dimension, metric="cosine"
        )
        return PineconeOnlineTable(
            client=self.client,
            index_name=self._create_index_name(feature, variant),
            namespace=self._create_namespace(feature, variant),
            vector_type=vector_type,
        )

    def _create_index_name(self, feature: str, variant: str) -> str:
        index_uuid = uuid.uuid5(uuid.NAMESPACE_DNS, f"{feature}-{variant}")
        return self.index_name_template.format(index_uuid)

    def _create_namespace(self, feature: str, variant: str) -> str:
        return f"ff-namespace--{feature}-{variant}"

from abc import ABC, abstractmethod
from typing import List, Any, Union

import numpy as np

from ..enums import ScalarType


class ValueType(ABC):
    @abstractmethod
    def scalar(self):
        pass

    @abstractmethod
    def is_vector(self):
        pass


class Scalar(ValueType):
    def __init__(self, scalar_type: ScalarType):
        self.scalar_type = scalar_type

    def scalar(self):
        return self.scalar_type

    def is_vector(self):
        return False


class VectorType(ValueType):
    def __init__(self, scalar_type: ScalarType, dimension: int, is_embedding: bool):
        self.scalar_type = scalar_type
        self.dimension = dimension
        self.is_embedding = is_embedding

    def scalar(self):
        return self.scalar_type

    def is_vector(self):
        return True


class OnlineStore(ABC):
    @abstractmethod
    def get_table(self, feature: str, variant: str):
        pass

    @abstractmethod
    def create_table(self, feature: str, variant: str, entity_type: ValueType):
        pass

    @abstractmethod
    def delete_table(self, feature: str, variant: str):
        pass

    @abstractmethod
    def table_exists(self, name, variant):
        pass

    @abstractmethod
    def close(self):
        pass


class OnlineStoreTable(ABC):
    @abstractmethod
    def set(self, entity: str, value: object):
        pass

    @abstractmethod
    def get(self, entity: str):
        pass


class VectorStoreTable(OnlineStoreTable, ABC):
    @abstractmethod
    def nearest(
        self, feature: str, variant: str, vector: Union[List[float], np.ndarray], k: int
    ):
        pass


class VectorStore(OnlineStore, ABC):
    @abstractmethod
    def create_index(self, feature: str, variant: str, vector_type: VectorType):
        pass

    @abstractmethod
    def delete_index(self, feature: str, variant: str):
        pass

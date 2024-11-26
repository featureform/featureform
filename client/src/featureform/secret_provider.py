from __future__ import annotations

import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Generic, TypeVar, Union


class SecretType(str, Enum):
    ENVIRONMENT = "environment"
    STATIC = "static"


S = TypeVar("S", bound="Secret")


class SecretProvider(Generic[S], ABC):
    @abstractmethod
    def secret(self, key: str, **kwargs) -> S:
        """Create a secret retriever for delayed access."""
        pass


@dataclass(frozen=True)
class EnvironmentSecretProvider(SecretProvider["EnvironmentSecret"]):
    """Creates references to environment variable secrets."""

    def secret(self, key: str, **kwargs) -> EnvironmentSecret:
        """Create an environment variable secret reference."""
        return EnvironmentSecret(key)


class Secret(ABC):
    """Abstract base class for secret values with delayed retrieval."""

    @abstractmethod
    def get(self) -> Any:
        """Retrieve the secret value."""
        pass

    @abstractmethod
    def serialize(self) -> Dict[str, Any]:
        """Serialize the secret configuration."""
        pass

    @classmethod
    def deserialize(cls, data: Union[Dict[str, Any] | Any]) -> Secret:
        """
        Deserialize a secret from a dictionary configuration.

        Args:
            data: Dictionary containing serialized secret data

        Returns:
            Appropriate Secret implementation

        Raises:
            ValueError: If the secret type is unknown
        """
        # Handle static values (backwards compatibility)
        if "type" not in data:
            return StaticSecret(data)

        secret_map = {
            SecretType.ENVIRONMENT.name: EnvironmentSecret,
            SecretType.STATIC.name: StaticSecret,
        }

        secret_type = data["type"]
        if secret_type.upper() not in secret_map:
            raise ValueError(f"Unknown secret type: {secret_type}")

        return secret_map[secret_type].deserialize(data)


@dataclass(frozen=True)
class StaticSecret(Secret):
    """Secret containing a static value."""

    value: Any

    def get(self) -> Any:
        return self.value

    def serialize(self) -> str:
        return self.value

    @classmethod
    def deserialize(cls, data: Dict[str, Any] | str) -> StaticSecret:
        if not isinstance(data, Dict):
            return cls(data)  # Backwards compatibility
        return cls(data.get("value", data))


@dataclass(frozen=True)
class EnvironmentSecret(Secret):
    """Secret that retrieves its value from environment variables."""

    key: str

    def get(self) -> str | None:
        return os.getenv(self.key)

    def serialize(self) -> Dict[str, str]:
        return {"type": SecretType.ENVIRONMENT.name, "key": self.key}

    @classmethod
    def deserialize(cls, data: Dict[str, Any]) -> EnvironmentSecret:
        if "key" not in data:
            raise ValueError(
                "EnvironmentSecret deserialization failed: 'key' is missing."
            )
        return cls(data["key"])

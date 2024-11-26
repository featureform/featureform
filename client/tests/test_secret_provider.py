from typing import Generator
from unittest.mock import Mock

import pytest

from featureform import PostgresConfig, Registrar
from featureform.proto.metadata_pb2 import ProviderRequest
from featureform.secret_provider import (
    EnvironmentSecret,
    EnvironmentSecretProvider,
    Secret,
    SecretType,
    StaticSecret,
)


@pytest.fixture
def registrar():
    return Registrar()


def test_integration(registrar):
    remote_env = EnvironmentSecretProvider()

    pg = registrar.register_postgres(
        name="test-postgres",
        host="host",
        port="port",
        database="database",
        user="username",
        password=remote_env.secret("password"),
    )

    stub = Mock()

    pg._provider()._create(None, stub)

    stub.CreateProvider.assert_called_once()
    called_args = stub.CreateProvider.call_args
    provider_request: ProviderRequest = called_args[0][0]
    serialized_config = provider_request.provider.serialized_config
    expected_pg_config = PostgresConfig(
        host="host",
        port="port",
        database="database",
        user="username",
        password=remote_env.secret("password"),
        sslmode="disable",
    )

    assert PostgresConfig.deserialize(serialized_config) == expected_pg_config


@pytest.fixture
def env_manager() -> EnvironmentSecretProvider:
    """Fixture providing an EnvironmentSecretProvider instance."""
    return EnvironmentSecretProvider()


@pytest.fixture
def mock_env_vars(monkeypatch) -> Generator[None, None, None]:
    """Fixture setting up test environment variables."""
    test_vars = {
        "TEST_SECRET": "secret_value",
    }
    for key, value in test_vars.items():
        monkeypatch.setenv(key, value)
    yield
    for key in test_vars:
        monkeypatch.delenv(key)


class TestEnvironmentSecretProvider:
    def test_secret_creation(self, env_manager: EnvironmentSecretProvider) -> None:
        """Test creation of environment secret."""
        secret = env_manager.secret("TEST_KEY")
        assert isinstance(secret, Secret)
        assert secret.key == "TEST_KEY"


class TestStaticSecret:
    def test_get_value(self) -> None:
        """Test static secret value retrieval."""
        secret = StaticSecret("test_value")
        assert secret.get() == "test_value"


class TestEnvironmentSecret:
    def test_get_value(self, mock_env_vars) -> None:
        """Test environment secret value retrieval."""
        secret = EnvironmentSecret("TEST_SECRET")
        assert secret.get() == "secret_value"

        # Test nonexistent key
        secret = EnvironmentSecret("NONEXISTENT")
        assert secret.get() is None


class TestSerde:
    """Tests for serialization and deserialization of secrets."""

    def test_static_secret_serde(self) -> None:
        """Test StaticSecret serialization/deserialization."""
        # Standard format
        original = StaticSecret("test_value")
        serialized = original.serialize()
        deserialized = Secret.deserialize(serialized)

        assert isinstance(deserialized, StaticSecret)
        assert deserialized.get() == original.get()
        assert original == deserialized

        # Legacy format (direct value)
        legacy_secret = Secret.deserialize("direct_value")
        assert isinstance(legacy_secret, StaticSecret)
        assert legacy_secret.get() == "direct_value"

        # Legacy format (dictionary)
        dict_data = {"key": "value"}
        dict_secret = Secret.deserialize(dict_data)
        assert isinstance(dict_secret, StaticSecret)
        assert dict_secret.get() == dict_data

    def test_environment_secret_serde(self) -> None:
        """Test EnvironmentSecret serialization/deserialization."""
        original = EnvironmentSecret("TEST_KEY")
        serialized = original.serialize()
        deserialized = Secret.deserialize(serialized)

        assert isinstance(deserialized, EnvironmentSecret)
        assert original == deserialized

    def test_invalid_secret_type(self) -> None:
        """Test error handling for invalid secret type."""
        with pytest.raises(ValueError, match="Unknown secret type: INVALID"):
            Secret.deserialize({"type": "INVALID"})

    @pytest.mark.parametrize(
        "secret_type,expected_class,test_data",
        [
            (SecretType.ENVIRONMENT.name, EnvironmentSecret, {"key": "TEST_KEY"}),
            (SecretType.STATIC.name, StaticSecret, {"value": "test_value"}),
        ],
    )
    def test_secret_type_serde(
        self, secret_type: str, expected_class: type, test_data: dict
    ) -> None:
        """Test serialization/deserialization for different secret types."""
        data = {"type": secret_type, **test_data}
        secret = Secret.deserialize(data)
        assert isinstance(secret, expected_class)

        # Test round trip
        reserialized = secret.serialize()
        assert Secret.deserialize(reserialized).__class__ == expected_class

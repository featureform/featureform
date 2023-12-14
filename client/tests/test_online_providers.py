from featureform.providers import (
    get_provider,
    OnlineStore,
    OnlineStoreTable,
    LocalFileStore,
)
from featureform.providers.online_store import ValueType, Scalar
from featureform.register import ScalarType
import pytest
import uuid
from datetime import datetime


@pytest.mark.parametrize(
    "provider",
    ["LOCAL_ONLINE"],
)
class TestOnlineProvider:
    def test_get_provider(self, provider):
        assert isinstance(get_provider(provider)(), LocalFileStore)
        with pytest.raises(NotImplementedError):
            get_provider("something else")

    @pytest.mark.parametrize(
        "name,variant,t",
        [(str(uuid.uuid4()), str(uuid.uuid4()), Scalar(ScalarType.INT))],
    )
    def test_create_table(self, provider, name, variant, t):
        store = get_provider(provider)()
        assert isinstance(store, OnlineStore)
        table = store.create_table(name, variant, entity_type=t)
        assert isinstance(table, OnlineStoreTable)

    @pytest.mark.parametrize(
        "name,variant,t",
        [(str(uuid.uuid4()), str(uuid.uuid4()), Scalar(ScalarType.INT))],
    )
    def test_get_table(self, provider, name, variant, t):
        store = get_provider(provider)()
        store.create_table(name=name, variant=variant, entity_type=t)
        assert isinstance(store, OnlineStore)
        table = store.get_table(name, variant)
        assert isinstance(table, OnlineStoreTable)

    @pytest.mark.parametrize(
        "name,variant,values,t",
        [
            (
                str(uuid.uuid4()),
                str(uuid.uuid4()),
                [("a", "one"), ("b", "two"), ("c", "three")],
                Scalar(ScalarType.STRING),
            ),
            (
                str(uuid.uuid4()),
                str(uuid.uuid4()),
                [("a", 1), ("b", 2), ("c", 3)],
                Scalar(ScalarType.INT),
            ),
            (
                str(uuid.uuid4()),
                str(uuid.uuid4()),
                [("a", 1.0), ("b", 2.0), ("c", 3.0)],
                Scalar(ScalarType.FLOAT64),
            ),
            (
                str(uuid.uuid4()),
                str(uuid.uuid4()),
                [("a", True), ("b", False), ("c", True)],
                Scalar(ScalarType.BOOL),
            ),
            (
                str(uuid.uuid4()),
                str(uuid.uuid4()),
                [("a", datetime.now()), ("b", datetime.now()), ("c", datetime.now())],
                Scalar(ScalarType.DATETIME),
            ),
        ],
    )
    def test_set_get_value(self, provider, name, variant, values, t):
        store = get_provider(provider)
        table = store().create_table(name=name, variant=variant, entity_type=t)
        for value in values:
            table.set(value[0], value[1])
        for value in values:
            assert table.get(value[0]) == value[1], f"Failed for entity {value[0]}"

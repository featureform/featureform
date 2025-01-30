import sys
import pytest

sys.path.insert(0, "client/src/")
import featureform as ff
from test_client import snowflake_fields, postgres_fields, MockStub
from featureform.resources import LabelVariant


class MockSourceVariant:
    def name_variant(self):
        return ("mock_name", "mock_variant")


def test_register_label_on_non_snowflake_provider():
    client = ff.Client(host="localhost:7878", insecure=True, dry_run=True)
    client._stub = MockStub()

    psql = ff.register_postgres(
        name=postgres_fields["name"],
        host=postgres_fields["host"],
        port=postgres_fields["port"],
        user=postgres_fields["user"],
        password=postgres_fields["password"],
        database=postgres_fields["database"],
        description=postgres_fields["description"],
        team=postgres_fields["team"],
        sslmode=postgres_fields["sslmode"],
        tags=postgres_fields["tags"],
        properties=postgres_fields["properties"],
    )

    assert hasattr(psql, "register_label")
    with pytest.raises(
        ValueError,
        match="Registering labels on SQL offline providers is currently only supported for Snowflake",
    ):
        psql.register_label(
            name="label",
            entity_mappings=[],
            value_type=ff.Bool,
            dataset=None,
            value_column="value",
        )


@pytest.mark.parametrize(
    "name,entity_mappings,value_type,dataset,value_column,expected_exception,expected_message",
    [
        (
            "label",
            None,
            None,
            None,
            "",
            ValueError,
            "entity_mappings must be a non-empty list",
        ),
        (
            "label",
            [],
            None,
            None,
            "",
            ValueError,
            "entity_mappings must be a non-empty list",
        ),
        (
            "label",
            [("entity", "column")],
            None,
            None,
            "",
            ValueError,
            "entity_mappings must be a list of dictionaries",
        ),
        (
            "label",
            [{"entity": "entity"}],
            None,
            None,
            "",
            ValueError,
            "missing entity column in mapping",
        ),
        (
            "label",
            [{"column": "column"}],
            None,
            None,
            "",
            ValueError,
            "missing entity name in mapping",
        ),
        (
            "label",
            [{"column": "column", "entity": "entity"}],
            None,
            None,
            "",
            ValueError,
            "Invalid type for label",
        ),
        (
            "label",
            [{"column": "column", "entity": "entity"}],
            ff.Bool,
            None,
            "",
            ValueError,
            "Dataset must have a name_variant method",
        ),
        (
            "label",
            [{"column": "column", "entity": "entity"}],
            ff.Bool,
            MockSourceVariant(),
            "",
            ValueError,
            "value_column must be provided",
        ),
    ],
)
def test_required_params(
    name,
    entity_mappings,
    value_type,
    dataset,
    value_column,
    expected_exception,
    expected_message,
):
    client = ff.Client(host="localhost:7878", insecure=True, dry_run=True)
    client._stub = MockStub()

    sf = ff.register_snowflake(
        name=snowflake_fields["name"],
        username=snowflake_fields["username"],
        password=snowflake_fields["password"],
        account=snowflake_fields["account"],
        organization=snowflake_fields["organization"],
        database=snowflake_fields["database"],
        warehouse=snowflake_fields["warehouse"],
        description=snowflake_fields["description"],
        role=snowflake_fields["role"],
        team=snowflake_fields["team"],
        tags=snowflake_fields["tags"],
        properties=snowflake_fields["properties"],
    )

    assert hasattr(sf, "register_label")
    with pytest.raises(expected_exception, match=expected_message):
        sf.register_label(
            name=name,
            entity_mappings=entity_mappings,
            value_type=value_type,
            dataset=dataset,
            value_column=value_column,
        )


def test_return_type():
    client = ff.Client(host="localhost:7878", insecure=True, dry_run=True)
    client._stub = MockStub()

    sf = ff.register_snowflake(
        name=snowflake_fields["name"],
        username=snowflake_fields["username"],
        password=snowflake_fields["password"],
        account=snowflake_fields["account"],
        organization=snowflake_fields["organization"],
        database=snowflake_fields["database"],
        warehouse=snowflake_fields["warehouse"],
        description=snowflake_fields["description"],
        role=snowflake_fields["role"],
        team=snowflake_fields["team"],
        tags=snowflake_fields["tags"],
        properties=snowflake_fields["properties"],
    )

    assert hasattr(sf, "register_label")
    lbl = sf.register_label(
        name="label",
        entity_mappings=[{"column": "column", "entity": "entity"}],
        value_type=ff.Bool,
        dataset=MockSourceVariant(),
        value_column="transaction_amount",
    )

    assert isinstance(lbl, LabelVariant)

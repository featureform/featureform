import pytest
from unittest.mock import patch, MagicMock
import pyarrow as pa
from iceberg_streamer import StreamerService
import json

@pytest.fixture(scope="module")
def streamer_service():
    return StreamerService()

@pytest.mark.local
@pytest.mark.parametrize("ticket_input", [
    "invalid_format",
    "\\{haha\\}",
])
def test_do_get_invalid_ticket_format(ticket_input, streamer_service):
    invalid_ticket = MagicMock()
    invalid_ticket.ticket.decode.return_value = ticket_input

    with pytest.raises(ValueError, match="Invalid JSON format in ticket"):
        streamer_service.do_get("default", invalid_ticket)

@pytest.mark.local
@pytest.mark.parametrize(
    "ticket_input, expectedError",
    [
        ('{}', "Missing required request fields: namespace, table, client.access-key-id, client.secret-access-key, client.region"),
        ('{"namespace": "my_namespace"}', "Missing required request fields: table, client.access-key-id, client.secret-access-key, client.region"),
        ('{"namespace": "my_namespace", "table": "my_table"}', "Missing required request fields: client.access-key-id, client.secret-access-key, client.region"),
        ('{"namespace": "my_namespace", "table": "my_table"}', "Missing required request fields: client.access-key-id, client.secret-access-key, client.region"),
        ('{"namespace": "my_namespace", "table": "my_table", "client.access-key-id": "my_key"}', "Missing required request fields: client.secret-access-key, client.region"),
        ('{"namespace": "my_namespace", "table": "my_table", "client.access-key-id": "my_key", "client.secret-access-key": "my_access"}', "Missing required request fields: client.region"),
    ],
)
def test_do_get_missing_fields(ticket_input, expectedError, streamer_service):
    invalid_ticket = MagicMock()
    invalid_ticket.ticket.decode.return_value = ticket_input

    with pytest.raises(ValueError, match=expectedError):
        streamer_service.do_get(None, invalid_ticket)




@patch("iceberg_streamer.load_catalog")
@patch("os.getenv", side_effect=lambda key, default=None: "test-uri" if key == "PYICEBERG_CATALOG__DEFAULT__URI" else default)
def test_do_get_success_fires_correct_params(_, mock_load_catalog, streamer_service):
    mock_catalog = MagicMock()
    mock_table = MagicMock()

    # need to create a minimal reader to keep pyarrow happy (mock won't suffice)
    schema = pa.schema([("col1", pa.int32()), ("col2", pa.string())])
    batch = pa.RecordBatch.from_arrays([pa.array([1, 2, 3]), pa.array(["a", "b", "c"])], schema=schema)
    table = pa.Table.from_batches([batch])
    record_batch_reader = table.to_reader()

    mock_table.scan.return_value.to_arrow_batch_reader.return_value = record_batch_reader
    mock_catalog.load_table.return_value = mock_table
    mock_load_catalog.return_value = mock_catalog

    ticket_data = {
        "catalog": "my_catalog",
        "namespace": "my_namespace",
        "table": "my_table",
        "client.region": "my_region",
        "client.access-key-id": "my_key",
        "client.secret-access-key": "my_access",
    }

    flight_ticket = MagicMock()
    flight_ticket.ticket.decode.return_value = json.dumps(ticket_data)

    # fire the request
    response = streamer_service.do_get("default", flight_ticket)

    assert isinstance(response, pa.flight.RecordBatchStream)
    mock_load_catalog.assert_called_once_with(
        "my_catalog", 
        **{"type": "glue", "client.region": "my_region", "client.access-key-id": "my_key", "client.secret-access-key": "my_access" }
    )
    mock_catalog.load_table.assert_called_once_with(("my_namespace", "my_table"))



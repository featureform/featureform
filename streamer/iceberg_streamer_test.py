import pytest
from unittest.mock import patch, MagicMock
import re
import pyarrow as pa
from iceberg_streamer import StreamerService, logger

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
@pytest.mark.parametrize("ticket_input", [
    '{}',
    '{"myKey": "myValue"}',
    '{"namespace": "my_namespace", "table": "", "s3.access-key-id": "", "s3.secret-access-key": ""}',
    '{"namespace": "", "table": "my_table", "s3.access-key-id": "", "s3.secret-access-key": ""}',
    '{"namespace": "", "table": "", "s3.access-key-id": "my_key", "s3.secret-access-key": ""}',
    '{"namespace": "", "table": "", "s3.access-key-id": "", "s3.secret-access-key": "my_secret"}',
])
def test_do_get_empty_namespace_or_table(ticket_input, streamer_service):
    invalid_ticket = MagicMock()
    invalid_ticket.ticket.decode.return_value = ticket_input

    with pytest.raises(Exception, match="Missing required fields in JSON:"):
        streamer_service.do_get("default", invalid_ticket)

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

    flight_ticket = MagicMock()
    flight_ticket.ticket.decode.return_value = '{"catalog": "my_catalog", "namespace": "my_namespace", "table": "my_table", "s3.region": "my_region", "s3.access-key-id": "my_key", "s3.secret-access-key": "my_access"}'

    # fire the request
    response = streamer_service.do_get("default", flight_ticket)

    assert isinstance(response, pa.flight.RecordBatchStream)
    mock_load_catalog.assert_called_once_with("my_catalog", **{"type": "glue", "s3.region": "my_region", "s3.access-key-id": "my_key", "s3.secret-access-key": "my_access" })
    mock_catalog.load_table.assert_called_once_with(("my_namespace", "my_table"))

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
        streamer_service.do_get(None, invalid_ticket)

@pytest.mark.local
@pytest.mark.parametrize("ticket_input", [
    '{}',
    '{"myKey": "myValue"}',
    '{"namespace": "my_namespace", "table": ""}',
    '{"namespace": "", "table": "my_table"}',
])
def test_do_get_empty_namespace_or_table(ticket_input, streamer_service):
    invalid_ticket = MagicMock()
    invalid_ticket.ticket.decode.return_value = ticket_input

    with pytest.raises(Exception, match="Missing 'namespace' or 'table' in JSON"):
        streamer_service.do_get(None, invalid_ticket)

@patch("os.getenv", side_effect=lambda key, default=None: None if key == "PYICEBERG_CATALOG__DEFAULT__URI" else default)
def test_load_data_missing_catalog_uri(_, streamer_service):
    with pytest.raises(EnvironmentError, match="Environment variable 'PYICEBERG_CATALOG__DEFAULT__URI' is not set."):
        streamer_service.load_data_from_iceberg_table("namespace", "table")


@patch("iceberg_streamer.load_catalog")
@patch("os.getenv", side_effect=lambda key, default=None: "test-uri" if key == "PYICEBERG_CATALOG__DEFAULT__URI" else default)
def test_do_get_success_fires_correct_params(_, mock_load_catalog, streamer_service):
    mock_catalog = MagicMock()
    mock_table = MagicMock()

    # need to create a minimal reader to keep pyarrow happy
    schema = pa.schema([("col1", pa.int32()), ("col2", pa.string())])
    batch = pa.RecordBatch.from_arrays([pa.array([1, 2, 3]), pa.array(["a", "b", "c"])], schema=schema)
    table = pa.Table.from_batches([batch])
    record_batch_reader = table.to_reader()

    mock_table.scan.return_value.to_arrow_batch_reader.return_value = record_batch_reader
    mock_catalog.load_table.return_value = mock_table
    mock_load_catalog.return_value = mock_catalog

    flight_ticket = MagicMock()
    flight_ticket.ticket.decode.return_value = '{"namespace": "my_namespace", "table": "my_table"}'

    # fire the request
    response = streamer_service.do_get(None, flight_ticket)

    assert isinstance(response, pa.flight.RecordBatchStream)
    mock_load_catalog.assert_called_once_with(None, **{"type": "glue", "s3.region": "us-east-1"})
    mock_catalog.load_table.assert_called_once_with(("my_namespace", "my_table"))

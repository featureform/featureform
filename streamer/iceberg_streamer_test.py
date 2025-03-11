#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

import pytest
from unittest.mock import patch, MagicMock
import pyarrow as pa
import json


@pytest.mark.local
@pytest.mark.parametrize(
    "ticket_input",
    [
        "invalid_format",
        "\\{haha\\}",
    ],
)
def test_do_get_invalid_ticket_format(ticket_input, streamer_service):
    invalid_ticket = MagicMock()
    invalid_ticket.ticket.decode.return_value = ticket_input

    with pytest.raises(ValueError, match="Invalid JSON format in ticket"):
        streamer_service.do_get("default", invalid_ticket)


@pytest.mark.local
@pytest.mark.parametrize(
    "ticket_input, expected_error",
    [
        (
            "{}",
            "Missing required request fields: namespace, table",
        ),
        (
            '{"namespace": "my_namespace"}',
            "Missing required request fields: table",
        ),
        (
            '{"namespace": "my_namespace", "table": "my_table", "client.region": "some_region"}',
            "Invalid credentials: Provide either 'client.access-key-id' and 'client.secret-access-key' or 'client.role-arn'."
        ),
        (
            '{"namespace": "my_namespace", "table": "my_table", "client.role-arn": "my_role"}',
            "Missing required Glue config fields: client.region",
        ),
        (
            '{"namespace": "my_namespace", "table": "my_table", "client.access-key-id": "my_key"}',
            "Missing required Glue config fields: client.region"
        ),
    ],
)
def test_do_get_missing_fields(ticket_input, expected_error, streamer_service):
    invalid_ticket = MagicMock()
    invalid_ticket.ticket.decode.return_value = ticket_input

    with pytest.raises(ValueError, match=expected_error):
        streamer_service.do_get(None, invalid_ticket)


@pytest.mark.local
@pytest.mark.parametrize(
    "limit_input, expected_error",
    [
        (
            "PTY",
            "Invalid 'limit' value: PTY. Must be a positive integer value.",
        ),
        (
            0,
            "Invalid 'limit' value: 0. Must be a positive integer value.",
        ),
        (
            -1988,
            "Invalid 'limit' value: -1988. Must be a positive integer value.",
        ),
    ],
)
def test_do_get_limit_inputs(limit_input, expected_error, streamer_service):
    ticket_data = {
        "catalog": "my_catalog",
        "namespace": "my_namespace",
        "table": "my_table",
        "client.region": "my_region",
        "client.access-key-id": "my_key",
        "client.secret-access-key": "my_access",
        "limit": limit_input,
    }

    invalid_ticket = MagicMock()
    invalid_ticket.ticket.decode.return_value = json.dumps(ticket_data)

    with pytest.raises(ValueError, match=expected_error):
        streamer_service.do_get(None, invalid_ticket)


@patch("iceberg_streamer.load_catalog")
def test_do_get_success_fires_correct_params(mock_load_catalog, streamer_service):
    mock_catalog = MagicMock()
    mock_table = MagicMock()

    # need to create a minimal reader to keep pyarrow happy (mock won't suffice)
    schema = pa.schema([("col1", pa.int32()), ("col2", pa.string())])
    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3]), pa.array(["a", "b", "c"])], schema=schema
    )
    table = pa.Table.from_batches([batch])
    record_batch_reader = table.to_reader()

    mock_table.scan.return_value.to_arrow_batch_reader.return_value = (
        record_batch_reader
    )
    mock_catalog.load_table.return_value = mock_table
    mock_load_catalog.return_value = mock_catalog

    ticket_data = {
        "catalog": "my_catalog",  # optional
        "namespace": "my_namespace",
        "table": "my_table",
        "client.region": "my_region",
        "client.access-key-id": "my_key",
        "client.secret-access-key": "my_access",
        "limit": 1,  # optional
    }

    flight_ticket = MagicMock()
    flight_ticket.ticket.decode.return_value = json.dumps(ticket_data)

    # fire the request
    response = streamer_service.do_get("default", flight_ticket)

    assert isinstance(response, pa.flight.RecordBatchStream)
    mock_load_catalog.assert_called_once_with(
        "my_catalog",
        **{
            "type": "glue",
            "client.region": "my_region",
            "client.access-key-id": "my_key",
            "client.secret-access-key": "my_access",
            "client.role-arn": None,
        }
    )
    mock_catalog.load_table.assert_called_once_with(("my_namespace", "my_table"))

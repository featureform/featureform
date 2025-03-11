#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2025 FeatureForm Inc.
#
import os
import pytest
import json
from unittest.mock import MagicMock
import pyarrow as pa
import pyarrow.flight as fl
from pyiceberg.catalog import load_catalog

def test_glue_integration(streamer_client):
    """
    Verifies that all required environment variables are set,
    and that a 'transactions' table can be loaded from Glue.
    """
    # 1) Ensure all required environment variables are present
    required_env_vars = [
        "AWS_SECRET_KEY",
        "AWS_ACCESS_KEY_ID",
        "AWS_GLUE_REGION",
        "AWS_GLUE_DATABASE",
        "S3_BUCKET_REGION",
        "S3_BUCKET_NAME"
    ]
    missing_vars = [var for var in required_env_vars if not os.environ.get(var)]
    if missing_vars:
        pytest.fail(f"Missing required environment variables: {', '.join(missing_vars)}")
    db_name = os.environ["AWS_GLUE_DATABASE"]
    table_name = "transactions"
    ticket_data = {
        "namespace": db_name,
        "table": "transactions",
        "client.region": os.environ["AWS_GLUE_REGION"],
        "client.access-key-id": os.environ["AWS_ACCESS_KEY_ID"],
        "client.secret-access-key": os.environ["AWS_SECRET_KEY"],
        "limit": 10,
    }
    ticket_bytes = json.dumps(ticket_data).encode("utf-8")
    descriptor = fl.FlightDescriptor.for_command(ticket_bytes)
    schema = streamer_client.get_schema(descriptor).schema
    columns = [
        ("TransactionID", 9),
        ("CustomerID", 10),
        ("CustomerDOB", 11),
        ("CustLocation", 12),
        ("CustAccountBalance", 13),
        ("TransactionAmount", 14),
        ("Timestamp", 15),
        ("IsFraud", 16),
    ]
    expected_schema = pa.schema([
        pa.field(name, pa.large_string(), metadata={"PARQUET:field_id": str(field_id)})
        for name, field_id in columns
    ])
    assert schema == expected_schema

    ticket = fl.Ticket(ticket_bytes)
    arrow_reader = streamer_client.do_get(ticket)
    result_table = arrow_reader.read_all()
    received_data = result_table.to_pydict()
    print(f"Successfully read {len(received_data)} row(s) from '{db_name}.{table_name}'")
    expected = {
            'TransactionID': ['T31270', 'T42841', 'T45632', 'T43999', 'T32732', 'T5745', 'T5629', 'T39898', 'T45644', 'T44248'],
            'CustomerID': ['C1433534', 'C7233515', 'C5333538', 'C8233533', 'C8837983', 'C4715927', 'C3537961', 'C3530756', 'C6717858', 'C1434966'], 
            'CustomerDOB': ['9/3/73', '9/3/73', '9/3/73', '9/3/73', '21/9/86', '6/4/83', '21/9/86', '22/7/89', '1/6/78', '1/1/1800'], 
            'CustLocation': [None, None, None, None, '.', '.', '.', 'AMB', 'GOA', 'GOA'],
            'CustAccountBalance': ['2780.38', '2780.38', '2780.38', '2780.38', '153455.63', '11357.95', '153455.63', '2827.24', '274001.97', '44081.63'],
            'TransactionAmount': ['75', '14', '233', '149', '1875', '469', '826.25', '55', '399', '3000'],
            'Timestamp': ['2022-04-12 12:52:20 UTC', '2022-04-11 03:20:14 UTC', '2022-04-06 03:15:13 UTC', '2022-04-01 20:27:17 UTC', '2022-04-21 04:36:29 UTC', '2022-04-10 12:58:52 UTC', '2022-04-02 08:46:23 UTC', '2022-04-10 03:17:06 UTC', '2022-04-05 04:17:28 UTC', '2022-04-16 13:12:22 UTC'],
            'IsFraud': ['false', 'false', 'false', 'false', 'false', 'false', 'false', 'false', 'false', 'false'],
    }
    assert received_data == expected

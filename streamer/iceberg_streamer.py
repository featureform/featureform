#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

import json
import sys
import signal
from pyiceberg.catalog import load_catalog
from pyarrow.flight import FlightServerBase, RecordBatchStream

port = 8085
ONE_MILLION_RECORD_LIMIT = 1_000_000

class StreamerService(FlightServerBase):
    def __init__(self):
        location = f"grpc://0.0.0.0:{port}"
        super(StreamerService, self).__init__(location)

    def do_get(self, _, ticket):
        ticket_json = ticket.ticket.decode("utf-8")
        print("Receiving flight ticket...")

        try:
            request_data = json.loads(ticket_json)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON format in ticket") from e

        request_dict = {
            "catalog": request_data.get("catalog", "default"),
            "namespace": request_data.get("namespace"),
            "table": request_data.get("table"),
            "client.access-key-id": request_data.get("client.access-key-id"),
            "client.secret-access-key": request_data.get("client.secret-access-key"),
            "client.region": request_data.get("client.region"),
            "limit": request_data.get("limit", ONE_MILLION_RECORD_LIMIT),
        }

        print(f"Receiving flight ticket for table: {request_dict['namespace']}.{request_dict['table']}")

        required_fields = [
            "namespace",
            "table",
            "client.access-key-id",
            "client.secret-access-key",
            "client.region",
        ]
        missing_fields = [
            field for field in required_fields if not request_dict.get(field)
        ]
        if missing_fields:
            raise ValueError(
                f"Missing required request fields: {', '.join(missing_fields)}"
            )

        # validate the limit
        limit = request_dict.get("limit")
        if limit is not None:
            if not isinstance(limit, int) or limit <= 0:
                raise ValueError(
                    f"Invalid 'limit' value: {limit}. Must be a positive integer value."
                )

        record_batch_reader = self.load_data_from_iceberg_table(request_dict)
        return RecordBatchStream(record_batch_reader)

    def load_data_from_iceberg_table(self, request_dict):
        print(
            f"Loading table: {request_dict['namespace']}.{request_dict['table']} with catalog: {request_dict['catalog']}"
        )

        try:
            catalog = load_catalog(
                request_dict["catalog"],
                **{
                    "type": "glue",
                    "client.region": request_dict["client.region"],
                    "client.access-key-id": request_dict["client.access-key-id"],
                    "client.secret-access-key": request_dict["client.secret-access-key"],
                },
            )

            iceberg_table = catalog.load_table(
                (request_dict["namespace"], request_dict["table"])
            )
        except Exception as e:
            error_msg = f"Failed to load table {request_dict['namespace']}.{request_dict['table']}: {str(e)}"
            print(error_msg)
            raise RuntimeError(error_msg) from e

        # return the record reader
        limit = request_dict["limit"]
        scan = iceberg_table.scan(limit=limit)
        return scan.to_arrow_batch_reader()

def graceful_shutdown(server):
    print("Shutting down streamer service...")
    server.shutdown()
    sys.exit(0)

if __name__ == "__main__":
    print(f"Starting the streamer service on port {port}...")
    server = StreamerService()
    
    # close out gracefully
    signal.signal(signal.SIGINT, lambda sig, frame: graceful_shutdown(server))
    signal.signal(signal.SIGTERM, lambda sign, frame: graceful_shutdown(server))

    server.serve()
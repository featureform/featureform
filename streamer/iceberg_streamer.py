#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2024 FeatureForm Inc.
#

import json
from pyiceberg.catalog import load_catalog
from pyarrow.flight import FlightServerBase, RecordBatchStream
import logging

logger = logging.getLogger(__name__)
port = 8085


class StreamerService(FlightServerBase):
    def __init__(self):
        location = f"grpc://0.0.0.0:{port}"
        super(StreamerService, self).__init__(location)

    def do_get(self, _, ticket):
        ticket_json = ticket.ticket.decode("utf-8")
        logger.debug(f"do_get(): utf-8 ticket value: {ticket_json}")

        try:
            request_data = json.loads(ticket_json)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON format in ticket: {ticket_json}") from e

        requestDict = {
            "catalog": request_data.get("catalog", "default"),
            "namespace": request_data.get("namespace"),
            "table": request_data.get("table"),
            "client.access-key-id": request_data.get("client.access-key-id"),
            "client.secret-access-key": request_data.get("client.secret-access-key"),
            "client.region": request_data.get("client.region"),
            "limit": request_data.get("limit"),
        }

        required_fields = [
            "namespace",
            "table",
            "client.access-key-id",
            "client.secret-access-key",
            "client.region",
        ]
        missing_fields = [
            field for field in required_fields if not requestDict.get(field)
        ]
        if missing_fields:
            raise ValueError(
                f"Missing required request fields: {', '.join(missing_fields)}"
            )

        # validate the limit
        limit = request_data.get("limit")
        if limit is not None:
            if not isinstance(limit, int) or limit <= 0:
                raise ValueError(
                    f"Invalid 'limit' value: {limit}. Must be a positive integer value."
                )

        record_batch_reader = self.load_data_from_iceberg_table(requestDict)
        return RecordBatchStream(record_batch_reader)

    def load_data_from_iceberg_table(self, requestDict):
        logger.info(
            f"Loading table: {requestDict['namespace']}.{requestDict['table']} with catalog: {requestDict['catalog']}"
        )

        try:
            catalog = load_catalog(
                requestDict["catalog"],
                **{
                    "type": "glue",
                    "client.region": requestDict["client.region"],
                    "client.access-key-id": requestDict["client.access-key-id"],
                    "client.secret-access-key": requestDict["client.secret-access-key"],
                },
            )

            iceberg_table = catalog.load_table(
                (requestDict["namespace"], requestDict["table"])
            )
        except Exception as e:
            logger.error(
                f"Failed to load table {requestDict['namespace']}.{requestDict['table']}: {str(e)}"
            )
            raise

        # return the record reader
        limit = requestDict["limit"]
        scan = iceberg_table.scan(limit=limit)
        return scan.to_arrow_batch_reader()


if __name__ == "__main__":
    logger.info(f"Starting the streamer client service on port {port}...")
    server = StreamerService()
    server.serve()
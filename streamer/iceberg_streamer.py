import os
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

        catalog = request_data.get("catalog", "default")
        namespace = request_data.get("namespace")
        table = request_data.get("table")
        access_key = request_data.get("client.access-key-id")
        secret_key = request_data.get("client.secret-access-key")
        region = request_data.get("client.region", "us-east-1")

        if not all([namespace, table, access_key, secret_key]):
            raise ValueError(f"Missing required fields in JSON: {request_data}")

        requestDict = {
            "catalog": catalog,
            "namespace": namespace,
            "table": table,
            "client.access-key-id": access_key,
            "client.secret-access-key": secret_key,
            "client.region": region,
        }

        record_batch_reader = self.load_data_from_iceberg_table(requestDict)
        return RecordBatchStream(record_batch_reader)

    def load_data_from_iceberg_table(self, requestDict):
        logger.info(f'Loading table: {requestDict["namespace"]}.{requestDict["table"]}')
        
        catalog = load_catalog(requestDict["catalog"], **{"type": "glue",
                                           "client.region": requestDict["client.region"], 
                                           "client.access-key-id": requestDict["client.access-key-id"],
                                            "client.secret-access-key": requestDict["client.secret-access-key"]})
        
        iceberg_table = catalog.load_table((requestDict["namespace"], requestDict["table"]))
        scan = iceberg_table.scan()

        # return the record reader
        return scan.to_arrow_batch_reader()  


if __name__ == "__main__":
    logger.info(f"Starting the streamer client service on port {port}...")
    server = StreamerService()
    server.serve()

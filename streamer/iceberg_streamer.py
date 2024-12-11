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

        namespace = request_data.get("namespace")
        table = request_data.get("table")
        if not namespace or not table:
            raise ValueError(f"Missing 'namespace' or 'table' in JSON: {request_data}")

        record_batch_reader = self.load_data_from_iceberg_table(namespace, table)
        return RecordBatchStream(record_batch_reader)

    def load_data_from_iceberg_table(self, namespace, table_name):
        aws_region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
        os.environ["AWS_DEFAULT_REGION"] = aws_region  # set for boto

        catalog_uri = os.getenv('PYICEBERG_CATALOG__DEFAULT__URI')
        logger.debug(f"load_data_from_iceberg_table(): Catalog URI {catalog_uri}")
        if not catalog_uri:
            raise EnvironmentError("Environment variable 'PYICEBERG_CATALOG__DEFAULT__URI' is not set.")

        logger.info(f"Catalog URI:{catalog_uri}")
        logger.info(f"Loading table: {namespace}.{table_name}")
        # todo: update passed in catalog
        catalog = load_catalog("default", **{"type": "glue", "s3.region": aws_region})
        iceberg_table = catalog.load_table((namespace, table_name))

        scan = iceberg_table.scan() # todo: upper limit param? or keep tight scope for now?
        return scan.to_arrow_batch_reader()  # return the record reader

if __name__ == "__main__":
    logger.info(f"Starting the streamer client service on port {port}...")
    server = StreamerService()
    server.serve()

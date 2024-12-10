import os
from pyiceberg.catalog import load_catalog
from pyarrow.flight import FlightServerBase
import pyarrow as pa
import logging

logger = logging.getLogger(__name__)
port = 8085

class StreamerService(FlightServerBase):
    def __init__(self):
        location = f"grpc://0.0.0.0:{port}"
        super(StreamerService, self).__init__(location)

    def do_get(self, _, ticket):
        namespace_table = ticket.ticket.decode("utf-8")
        logger.debug(f"do_get(): utf-8 ticket value: {namespace_table}")
        if "." not in namespace_table:
            # todo: may change to regex
            raise ValueError(f"Invalid ticket format: {namespace_table}. Expected 'namespace.table_name'.")

        namespace, table = namespace_table.split(".")
        logger.debug(f"namespace split: {namespace}, {table}")
        if namespace is None or table is None:
            raise Exception(f"The namespace ({namespace}) or table name {table} variables are empty")
        
        return self.load_data_from_iceberg_table(namespace, table)

    def load_data_from_iceberg_table(self, namespace, table_name):
        catalog_uri = os.getenv('PYICEBERG_CATALOG__DEFAULT__URI')
        logger.debug(f"load_data_from_iceberg_table(): Catalog URI {catalog_uri}")
        if not catalog_uri:
            raise EnvironmentError("Environment variable 'PYICEBERG_CATALOG__DEFAULT__URI' is not set.")

        logger.info(f"Catalog URI:{catalog_uri}") 
        logger.info(f"Loading table: {namespace}.{table_name}")
        catalog = load_catalog(None, **{"type": "glue", "s3.region": "us-east-1"}) # todo: customizable or smaller scope for first pass?
        iceberg_table = catalog.load_table((namespace, table_name))

        scan = iceberg_table.scan() # todo: upper limit param? or keep tight scope for now?
        return scan.to_arrow_batch_reader() #return the record reader

if __name__ == "__main__":
    logger.info(f"Starting the streamer client service on port {port}...")
    server = StreamerService()
    server.serve()

#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
#  Copyright 2025 FeatureForm Inc.
#

import json
import sys
import signal
from abc import ABC, abstractmethod
from pyiceberg.catalog import load_catalog
from pyarrow.flight import FlightServerBase, RecordBatchStream, SchemaResult

port = 8085
DEFAULT_RECORD_LIMIT = 2_000_000
DEFAULT_CATALOG = "glue"

class StreamConfig(ABC):
    """
    Abstract base class for stream configuration.
    """
    def __init__(self, config):
        self.config = config

    @classmethod
    def from_ticket(cls, ticket) -> "StreamConfig":
        """
        Decode the flight ticket and create the appropriate StreamConfig instance.
        """
        try:
            config_data = json.loads(ticket.ticket.decode("utf-8"))
        except json.JSONDecodeError as e:
            raise ValueError("Invalid JSON format in ticket") from e

        return cls.from_dict(config_data)

    @classmethod
    def from_flight_command(cls, req) -> "StreamConfig":
        """
        Decode the command from a request and create the appropriate StreamConfig instance.
        """
        if not req.command:
            raise ValueError("Request must be a flight command.")
        try:
            config_data = json.loads(req.command.decode("utf-8"))
        except json.JSONDecodeError as e:
            raise ValueError("Invalid JSON format in ticket") from e
        return cls.from_dict(config_data)

    @classmethod
    def from_dict(cls, config_data: dict) -> "StreamConfig":
        required_fields = ["namespace", "table"]
        missing = [field for field in required_fields if not config_data.get(field)]
        if missing:
            raise ValueError(f"Missing required request fields: {', '.join(missing)}")

        limit = config_data.get("limit")
        if limit is not None:
            if not isinstance(limit, int) or limit <= 0:
                raise ValueError(
                    f"Invalid 'limit' value: {limit}. Must be a positive integer value."
                )

        catalog_type = config_data.get("catalog_type", "glue").lower()
        if catalog_type == "glue":
            return GlueConfig.parse(config_data)
        elif catalog_type == "":
            raise ValueError(f"catalog_type must be set")
        else:
            raise ValueError(f"Unsupported catalog type: {catalog}")

    def catalog_name(self):
        return self.config.get("catalog", "default")

    def namespace(self):
        return self.config.get("namespace")

    def table(self):
        return self.config.get("table")

    def limit(self):
        return self.config.get("limit", DEFAULT_RECORD_LIMIT)

    @abstractmethod
    def load_catalog_kwargs(self):
        """
        Return the parameters needed to load the catalog.
        """
        pass

class GlueConfig(StreamConfig):
    @classmethod
    def parse(cls, config_data: dict) -> "GlueConfig":
        required_fields = ["client.region"]
        missing = [field for field in required_fields if not config_data.get(field)]
        if missing:
            raise ValueError(f"Missing required Glue config fields: {', '.join(missing)}")
        has_static_credentials = config_data.get("client.access-key-id") and config_data.get("client.secret-access-key")
        has_role_arn = config_data.get("client.role-arn")
        if not (has_static_credentials or has_role_arn):
            raise ValueError(
                "Invalid credentials: Provide either 'client.access-key-id' and 'client.secret-access-key' or 'client.role-arn'."
            )
        return cls(config_data)

    def load_catalog_kwargs(self):
        return {
            "type": "glue",
            "client.region": self.config["client.region"], # not optional
            "client.access-key-id": self.config.get("client.access-key-id"),
            "client.secret-access-key": self.config.get("client.secret-access-key"),
            "client.role-arn": self.config.get("client.role-arn")
        }


class StreamerService(FlightServerBase):
    def __init__(self):
        location = f"grpc://0.0.0.0:{port}"
        super(StreamerService, self).__init__(location)

    def do_get(self, _, ticket):
        print("Receiving flight ticket...")
        config = StreamConfig.from_ticket(ticket)
        table = self.load_iceberg_table(config)
        batch_reader = table.scan(limit=config.limit()).to_arrow_batch_reader()
        print("Load complete. Returning batch reader to client.")
        return RecordBatchStream(batch_reader)

    def get_schema(self, _, req):
        print("Receiving get schema request...")
        config = StreamConfig.from_flight_command(req)
        table = self.load_iceberg_table(config)
        schema = table.schema().as_arrow()
        return SchemaResult(schema)

    def load_iceberg_table(self, config: StreamConfig):
        print(
            f"Loading table: {config.catalog_name()}.{config.namespace()}.{config.table()}"
        )
        try:
            catalog = load_catalog(
                config.catalog_name(),
                **config.load_catalog_kwargs(),
            )
            return catalog.load_table(
                (config.namespace(), config.table())
            )
        except Exception as e:
            error_msg = f"Failed to load table {catalog.namespace()}.{catalog.table()}: {str(e)}"
            print(error_msg)
            raise RuntimeError(error_msg) from e

def graceful_shutdown(server):
    print("Shutting down streamer service...")
    server.shutdown()
    sys.exit(0)

if __name__ == "__main__":
    print(f"Starting the streamer service on port {port}...")
    server = StreamerService()
    
    # close out gracefully
    signal.signal(signal.SIGINT, lambda _sig, _frame: graceful_shutdown(server))
    signal.signal(signal.SIGTERM, lambda _sign, _frame: graceful_shutdown(server))
    server.serve()

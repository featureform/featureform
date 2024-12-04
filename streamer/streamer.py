import os
import pyarrow.csv as csv
from pyarrow.flight import FlightServerBase, RecordBatchStream

class StreamerService(FlightServerBase):
    def __init__(self):
        location = "grpc://0.0.0.0:8085"
        super(StreamerService, self).__init__(location)

    def do_get(self, _, ticket):
        table_name = ticket.ticket.decode("utf-8")
        table = self.load_table_from_iceberg(table_name)

        return RecordBatchStream(table)

    def load_table_from_iceberg(self, table_name):
        print(f"Loading table: {table_name}")
        csv_path = f"./{table_name}.csv"
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"Table {table_name} not found at path: {csv_path}")

        # use pyarrow.csv to read the CSV file into an Arrow Table
        # but load in smaller chunks (2mb in this case)
        table = csv.read_csv(csv_path, read_options=csv.ReadOptions(block_size=2 * 1024 * 1024)) #2 mb chunks
        return table

if __name__ == "__main__":
    print("Starting the streamer client service...")
    server = StreamerService()
    server.serve()

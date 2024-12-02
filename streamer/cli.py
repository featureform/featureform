import pyarrow.flight as flight
import pyarrow as pa
import sys

def fetch_data_and_print(server_address, table_name):
    """
    Fetch data from the python-streamer
    """
    client = flight.connect(server_address)
    
    ticket = flight.Ticket(table_name.encode("utf-8"))

    # open flight stream with "ticket". kind of like the naming
    reader = client.do_get(ticket)

    schema = reader.schema
    print("Schema:")
    print(schema)

    # convert to pandas
    df = reader.read_pandas()
    print("\nTO PANDAS:")
    print(df)
    print(len(df))
    print(f"\nTotal rows fetched: {len(df)}")

if __name__ == "__main__":
    server_address = "grpc://localhost:8085"
    table_name = "table_data_short" if len(sys.argv) < 2 else sys.argv[1]

    print(f"Table Name: {table_name}")

    print(f"Connecting to Flight server at {server_address}")
    fetch_data_and_print(server_address, table_name)

import pandas as pd
import random
import string
from datetime import datetime

# Number of rows
num_rows = 10000

# Generate unique entity IDs
entity_ids = [f"ent_{i}" for i in range(1, num_rows + 1)]

# Generate random non-unique strings for the 'value' column
value_data = [''.join(random.choice("riddhisterlingaliahmaderikanthonycarlos") for _ in range(5)) for _ in range(num_rows)]

# Generate random non-unique numbers for the 'value' column
# value_data = [random.randrange(1, 1000) for _ in range(num_rows)]

# Generate timestamps for the 'ts' column
# timestamp_data = [datetime.now() for _ in range(num_rows)]

# Generate the 'row_number' column
row_number = list(range(1, num_rows + 1))

# Create a DataFrame
data = {
    'entity': entity_ids,
    'value': value_data,
    'row_number': row_number
}
df = pd.DataFrame(data)

# Save the DataFrame to a CSV file
df.to_parquet('test_stat_data_string.snappy.parquet', index=False, compression='snappy')

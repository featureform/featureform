#!/usr/bin/env python3
import boto3
import concurrent.futures


def get_tables_with_prefix(prefix):
    """
    Retrieve all DynamoDB tables with names starting with the given prefix.
    """
    dynamodb = boto3.client("dynamodb")
    tables = []

    response = dynamodb.list_tables()
    tables.extend(response.get("TableNames", []))

    # Handle pagination if needed
    while "LastEvaluatedTableName" in response:
        response = dynamodb.list_tables(
            ExclusiveStartTableName=response["LastEvaluatedTableName"]
        )
        tables.extend(response.get("TableNames", []))

    matching_tables = [table for table in tables if table.startswith(prefix)]
    return matching_tables


def delete_table(table_name):
    """
    Delete a single DynamoDB table and wait until deletion is confirmed.
    """
    dynamodb = boto3.client("dynamodb")
    try:
        print(f"Initiating deletion for table: {table_name}")
        dynamodb.delete_table(TableName=table_name)

        # Wait for table deletion to complete
        waiter = dynamodb.get_waiter("table_not_exists")
        print(f"Waiting for {table_name} to be deleted...")
        waiter.wait(TableName=table_name)
        print(f"Table {table_name} deleted successfully.")
    except Exception as e:
        print(f"Error deleting table {table_name}: {e}")


def main():
    prefix = "Featureform_table__"
    tables_to_delete = get_tables_with_prefix(prefix)

    if not tables_to_delete:
        print(f"No tables found with prefix '{prefix}'.")
        return

    print(f"Found {len(tables_to_delete)} table(s) with prefix '{prefix}':")
    for table in tables_to_delete:
        print(f"  - {table}")

    confirmation = (
        input(
            "\nAre you sure you want to delete these tables asynchronously? (yes/no): "
        )
        .strip()
        .lower()
    )
    if confirmation != "yes":
        print("Aborting deletion.")
        return

    # Launch deletion tasks concurrently using ThreadPoolExecutor
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {
            executor.submit(delete_table, table): table for table in tables_to_delete
        }
        for future in concurrent.futures.as_completed(futures):
            table = futures[future]
            try:
                future.result()
            except Exception as exc:
                print(f"Deletion generated an exception for table {table}: {exc}")


if __name__ == "__main__":
    main()

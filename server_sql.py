from psycopg2 import connect
from dotenv import load_dotenv
import os
from Postgres import Postgres

if __name__ == "__main__":
    # Define the path to your environment file containing Postgres credentials
    env_path = (
        ".env"  # Assuming your environment file is named ".env" in the same directory
    )

    # Load environment variables
    load_dotenv(env_path)

    # Create an instance of the Postgres class
    postgres = Postgres(env_path)

    # Connect to the Postgres database
    postgres.connect()

    # Create a database named "mydatabase" (uncomment if needed)
    # postgres.create_database("mydatabase")

    # Define the schema for a table named "mytable"
    table_schema = {"id": "int", "name": "text", "age": "int"}

    # Create the "mytable" table (uncomment if you don't have it already)
    # postgres.create_table("mytable", table_schema)

    # Prepare some data to insert
    data = {"id": 2, "name": "mohamed", "age": 28}

    # Insert the data into the table
    postgres.insert_data("mytable", data)

    # Execute a query to retrieve data from the table
    query = "SELECT * FROM mytable;"
    rows = postgres.execute_query(query)

    # Print the results
    for row in rows:
        print(row)

    # Disconnect from the Postgres database
    postgres.disconnect()

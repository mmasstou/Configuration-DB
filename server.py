from Cassandra import Cassandra

if __name__ == "__main__":
    # Define the path to your environment file containing Cassandra credentials
    env_path = (
        ".env"  # Assuming your environment file is named ".env" in the same directory
    )

    # Create an instance of the Cassandra class
    cassandra = Cassandra(env_path)

    # Connect to the Cassandra cluster
    cassandra.connect()

    # Create a keyspace named "mykeyspace" with a replication factor of 1
    # cassandra.create_keyspace("mykeyspace", 3)

    # Define the schema for a table named "mytable"
    table_schema = {"id": "int", "name": "text", "age": "int"}

    # Create the "mytable" table within the "mykeyspace" keyspace
    # cassandra.create_table("mykeyspace", "mytable", table_schema)

    # Prepare some data to insert
    data = {"id": 2, "name": "mohamed", "age": 28}

    # Insert the data into the table
    # cassandra.insert_data("mykeyspace", "mytable", data)

    # Execute a query to retrieve data from the table
    query = "SELECT * FROM mykeyspace.mytable;"
    rows = cassandra.execute_query(query)

    # Print the results
    for row in rows:
        print(row)

    # git table sechema :
    query = "DESCRIBE TABLE mykeyspace.mytable;"
    # Disconnect from the Cassandra cluster
    rows = cassandra.execute_query(query)
    # create_statemment = cassandra.generate_create_statement(
    #     keyspace_name="mykeyspace", table_name="mytable"
    # )

    for r in rows:
        print(f"+> {r}")

    # print(f"create_statemment : {create_statemment}")
    cassandra.disconnect()

    vcv = dict(
        keyspace_name="mykeyspace",
        type="table",
        name="mytable",
        create_statement="CREATE TABLE mykeyspace.mytable (\n    id int PRIMARY KEY,\n    age int,\n    name text\n) WITH additional_write_policy = '99p'\n    AND bloom_filter_fp_chance = 0.01\n    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}\n    AND cdc = false\n    AND comment = ''\n    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}\n    AND compression = {'chunk_length_in_kb': '16', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}\n    AND memtable = 'default'\n    AND crc_check_chance = 1.0\n    AND default_time_to_live = 0\n    AND extensions = {}\n    AND gc_grace_seconds = 864000\n    AND max_index_interval = 2048\n    AND memtable_flush_period_in_ms = 0\n    AND min_index_interval = 128\n    AND read_repair = 'BLOCKING'\n    AND speculative_retry = '99p';",
    )

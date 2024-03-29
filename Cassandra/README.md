## Cassandra Database Interaction Script

This Python script demonstrates interacting with a Cassandra database using the `cassandra-driver` library. It provides functionalities for connection management, keyspace and table creation, data insertion, querying, and schema retrieval.

**Requirements:**

- Python 3.x
- `cassandra-driver` library (installable via `pip install cassandra-driver`)
- `.env` file (optional) to store Cassandra credentials securely

**Setup:**

1. Install `cassandra-driver`:

   ```bash
   pip install cassandra-driver
   ```

2. Create a file named `.env` in the same directory as this script (optional). Add environment variables for your Cassandra credentials:

   ```
   CASSANDRA_USERNAME=your_username
   CASSANDRA_PASSWORD=your_password
   CASSANDRA_HOST=your_host  # defaults to localhost
   CASSANDRA_PORT=your_port  # defaults to 9042
   ```

3. Replace `"your_` values with your actual credentials.

**Usage:**

1. Modify the `env_path` variable in the script to point to your `.env` file location (if using).
2. Update the keyspace name (`keyspace_name`) and table schema (`table_schema`) according to your requirements.
3. Adjust the sample data (`data`) dictionary for your specific inserts.
4. Run the script:
   ```bash
   python cassandra_script.py
   ```

**Functionality:**

The script showcases the following functionalities of the `Cassandra` class:

- Connect to a Cassandra cluster using environment variables.
- Create a keyspace (if it doesn't exist).
- Create a table within a keyspace (if it doesn't exist).
- Insert data into a table.
- Execute a CQL (Cassandra Query Language) query and retrieve results.
- Retrieve the CREATE TABLE statement for an existing table (optional).
- Disconnect from the Cassandra cluster.

**Note:**

- This script provides a basic example. You can extend it to include additional functionalities like data deletion, updates, and more complex CQL queries.
- Ensure your Cassandra cluster configuration aligns with the script's assumptions (e.g., port).

**Additional Resources:**

- `cassandra-driver` documentation: [https://github.com/datastax/python-driver](https://github.com/datastax/python-driver)
- Cassandra Query Language (CQL): [https://docs.datastax.com/en/cql-oss/3.1/cql/cql_intro_c.html](https://docs.datastax.com/en/cql-oss/3.1/cql/cql_intro_c.html)

**Optional: Keyspace and Table Creation (Commented Out):**

The script includes commented-out sections for creating a keyspace and a table. Uncomment these sections if you want the script to create them automatically.

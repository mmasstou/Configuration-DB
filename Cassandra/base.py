import os
from cassandra.cluster import Cluster
from dotenv import load_dotenv
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

__all__ = ["Cassandra"]


class Cassandra:
    def __init__(self, env_path: str) -> None:
        """
        Initializes a Cassandra connection using environment variables.

        Args:
            env_path (str): Path to the environment file containing Cassandra credentials.
        """
        load_dotenv(env_path)

        self.username = os.getenv("CASSANDRA_USERNAME")
        self.password = os.getenv("CASSANDRA_PASSWORD")
        self.host = os.getenv("CASSANDRA_HOST", "localhost")
        self.port = int(os.getenv("CASSANDRA_PORT", 9042))

        self.cluster = None
        self.session = None

    def connect(self) -> None:
        """
        Establishes a connection to the Cassandra cluster.
        """
        auth_provider = PlainTextAuthProvider(
            username=self.username, password=self.password
        )
        # Set up connection to Cassandra cluster

        self.cluster = Cluster([self.host], port=self.port, auth_provider=auth_provider)
        self.session = self.cluster.connect()

    def create_keyspace(self, keyspace_name: str, replication_factor: int = 1) -> None:
        """
        Creates a keyspace if it doesn't already exist.

        Args:
            keyspace_name (str): Name of the keyspace to create.
            replication_factor (int, optional): Replication factor for the keyspace. Defaults to 1.
        """
        cql = f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace_name} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': {replication_factor}}};
        """
        self.session.execute(cql)

    def create_table(self, keyspace_name: str, table_name: str, schema: dict) -> None:
        """
        Creates a table if it doesn't already exist.

        Args:
            keyspace_name (str): Name of the keyspace.
            table_name (str): Name of the table to create.
            schema (dict): Dictionary defining the table schema. Key is column name, value is data type.
        """
        column_definitions = ", ".join(
            [f"{col} {data_type}" for col, data_type in schema.items()]
        )
        cql = f"""
        CREATE TABLE IF NOT EXISTS {keyspace_name}.{table_name} (
            {column_definitions},
            PRIMARY KEY ({", ".join(schema.keys())})
        );
        """
        self.session.execute(cql)

    def insert_data(self, keyspace_name: str, table_name: str, data: dict) -> None:
        """
        Inserts a single row of data into a table.

        Args:
            keyspace_name (str): Name of the keyspace.
            table_name (str): Name of the table.
            data (dict): Dictionary containing data to insert. Keys should match table columns.
        """
        placeholders = ", ".join([str(value) for value in data.values()])
        print(f"placeholders : {placeholders}")
        columns = ", ".join(data.keys())
        print(f"columns : {columns}")
        cql = f"""INSERT INTO {keyspace_name}.{table_name} ({columns}) VALUES ({placeholders});"""
        print(f"cql : {cql}")
        dd = list(data.values())
        print(f"data.values : {dd}")
        self.session.execute(str(cql))

    def execute_query(self, query: str):
        """
        Executes a CQL query and returns the results.

        Args:
            query (str): The CQL query to execute.

        Returns:
            cassandra.query.ResultSet: The results of the query.
        """
        return self.session.execute(query)

    def delete_table(self, keyspace_name: str, table_name: str) -> None:
        """
        Deletes a table from a keyspace.

        Args:
            keyspace_name (str): Name of the keyspace containing the table.
            table_name (str): Name of the table to delete.
        """
        cql = f"DROP TABLE {keyspace_name}.{table_name};"
        self.session.execute(cql)

    def delete_keyspace(self, keyspace_name: str) -> None:
        """
        Deletes a keyspace.

        Args:
            keyspace_name (str): Name of the keyspace to delete.
        """
        cql = f"DROP KEYSPACE {keyspace_name};"
        self.session.execute(cql)

    def update_data(
        self, keyspace_name: str, table_name: str, data: dict, where_clause: str
    ) -> None:
        """
        Updates data in a table based on a WHERE clause.

        Args:
            keyspace_name (str): Name of the keyspace containing the table.
            table_name (str): Name of the table.
            data (dict): Dictionary containing data to update. Keys are column names, values are new values.
            where_clause (str): WHERE clause specifying the rows to update.
        """
        set_expressions = ", ".join([f"{col} = ?" for col in data.keys()])
        columns = ", ".join(data.keys())
        cql = f"""
        UPDATE {keyspace_name}.{table_name}
        SET {set_expressions}
        WHERE {where_clause};
        """
        self.session.execute(cql, list(data.values()))

    def execute_script(self, script_path: str) -> None:
        """
        Executes a CQL script file.

        Args:
            script_path (str): Path to the CQL script file.
        """
        with open(script_path, "r") as f:
            cql_script = f.read()
        for statement in cql_script.splitlines():
            statement = statement.strip()  # Remove leading/trailing whitespace
            if not statement or statement.startswith(
                "--"
            ):  # Skip comments and empty lines
                continue
            self.execute_query(statement)

    def generate_create_statement(self, keyspace_name, table_name):
        query = "DESCRIBE TABLE {}.{}".format(keyspace_name, table_name)
        results = self.session.execute(query)
        if results:
            # Parse the output to create the CREATE TABLE statement
            create_statement = "CREATE TABLE IF NOT EXISTS {}.{} (".format(
                keyspace_name, table_name
            )
            first_column = True
            for result in results:
                column_name = result[0]
                data_type = result[1]
                primary_key = result[4] == "true"
                clustering_key = result[5] == "true"

                if first_column:
                    first_column = False
                    create_statement += " {} {} ".format(column_name, data_type)
                else:
                    create_statement += ", {} {} ".format(column_name, data_type)

                if primary_key:
                    create_statement += "PRIMARY KEY (" + column_name
                if clustering_key:
                    create_statement += ", " + column_name

            if primary_key or clustering_key:
                create_statement += ")"
            create_statement += ");"

            return create_statement

    def disconnect(self) -> None:
        """
        Closes the connection to the Cassandra cluster.
        """
        if self.session:
            self.session.shutdown()
        if self.cluster:
            self.cluster.shutdown()

import psycopg2
from dotenv import load_dotenv
import os


class Postgres:
    def __init__(self, env_path: str) -> None:
        """
        Initializes a Postgres connection using environment variables.

        Args:
            env_path (str): Path to the environment file containing Postgres credentials.
        """
        load_dotenv(env_path)

        self.username = os.getenv("POSTGRES_USERNAME")
        self.password = os.getenv("POSTGRES_PASSWORD")
        self.host = os.getenv("POSTGRES_HOST", "localhost")
        self.port = int(os.getenv("POSTGRES_PORT", 5432))
        self.database = os.getenv("POSTGRES_DATABASE")
        self.conn = None

    def connect(self) -> None:
        """
        Establishes a connection to the Postgres database.
        """
        try:
            self.conn = psycopg2.connect(
                user=self.username,
                password=self.password,
                host=self.host,
                port=self.port,
                database=self.database,
            )
            self.cursor = self.conn.cursor()
        except Exception as e:
            print(f"Error connecting to Postgres: {e}")
            raise

    def create_database(self, database_name: str) -> None:
        """
        Creates a database if it doesn't already exist.

        Args:
            database_name (str): Name of the database to create.
        """
        self.cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
        self.conn.commit()

    def create_table(self, table_name: str, schema: dict) -> None:
        """
        Creates a table if it doesn't already exist.

        Args:
            table_name (str): Name of the table to create.
            schema (dict): Dictionary defining the table schema. Key is column name, value is data type (Postgres compatible).
        """
        column_definitions = ", ".join(
            [f"{col} {data_type}" for col, data_type in schema.items()]
        )
        sql = f"""CREATE TABLE IF NOT EXISTS {table_name} ({column_definitions});"""
        self.cursor.execute(sql)
        self.conn.commit()

    def insert_data(self, table_name: str, data: dict) -> None:
        """
        Inserts a single row of data into a table.

        Args:
            table_name (str): Name of the table.
            data (dict): Dictionary containing data to insert. Keys should match table columns.
        """
        placeholders = ", ".join(["%s" for _ in data.values()])
        columns = ", ".join(data.keys())
        sql = f"""INSERT INTO {table_name} ({columns}) VALUES ({placeholders});"""
        self.cursor.execute(sql, list(data.values()))
        self.conn.commit()

    def execute_query(self, query: str, fetchall=True) -> list:
        """
        Executes a SQL query and returns the results.

        Args:
            query (str): The SQL query to execute.
            fetchall (bool, optional): Whether to fetch all results or just the first row. Defaults to True.

        Returns:
            list: List of rows containing the results.
        """
        self.cursor.execute(query)
        if fetchall:
            return self.cursor.fetchall()
        else:
            return self.cursor.fetchone()

    def update_data(self, table_name: str, data: dict, where_clause: str) -> None:
        """
        Updates data in a table based on a WHERE clause.

        Args:
            table_name (str): Name of the table.
            data (dict): Dictionary containing data to update. Keys are column names, values are new values.
            where_clause (str): WHERE clause specifying the rows to update.
        """
        set_expressions = ", ".join([f"{col} = %s" for col in data.keys()])
        sql = f"""UPDATE {table_name} SET {set_expressions} WHERE {where_clause};"""
        self.cursor.execute(sql, list(data.values()))
        self.conn.commit

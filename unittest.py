from Cassandra import Cassandra

# Mock cassandra interactions (optional)
# from unittest.mock import patch


class CassandraTest:

    def setUp(self):
        # Assuming a local Cassandra instance is available
        self.cassandra = Cassandra(".env")  # Replace with actual env path
        self.cassandra.connect()
        self.keyspace_name = "mykeyspace_test"
        self.table_name = "mytable_test"
        self.schema = {"id": "int", "name": "text", "age": "int"}

    def tearDown(self):
        self.cassandra.delete_keyspace(self.keyspace_name)
        self.cassandra.disconnect()

    # Test creating a keyspace
    def test_create_keyspace(self):
        self.cassandra.create_keyspace(self.keyspace_name)
        rows = self.cassandra.execute_query(f"DESCRIBE KEYSPACES;")
        keyspaces = [row[0] for row in rows]
        print(f"test_create_keyspace : {keyspaces}")
        
        # self.assertIn(self.keyspace_name, keyspaces)

    # Test creating a table
    def test_create_table(self):
        self.cassandra.create_keyspace(self.keyspace_name)
        self.cassandra.create_table(self.keyspace_name, self.table_name, self.schema)
        rows = self.cassandra.execute_query(
            f"DESCRIBE TABLES;"
        )
        tables = [row[0] for row in rows]
        print(f"test_create_table : {tables}")
        # self.assertIn(self.table_name, tables)



if __name__ == "__main__":
    Cassandratest = CassandraTest()
    Cassandratest.setUp()
    Cassandratest.test_create_keyspace()
    Cassandratest.test_create_table()
    Cassandratest.tearDown()

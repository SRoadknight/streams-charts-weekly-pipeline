import pyarrow as pa
import duckdb
import os

class DuckDBDataIngestor:
    def __init__(
        self,
        duckdb_schema: str,
        pyarrow_schema: pa.Schema,
        database_name: str,
        table_name: str,
        environment: str = "local"
    ):
        self.duckdb_schema = duckdb_schema
        self.pyarrow_schema = pyarrow_schema
        self.database_name = database_name
        self.table_name = table_name
        self.environment = environment
        self.conn = self.initialise_connection(environment)

    def initialise_connection(self, environment):
        """Initialise database connection."""

        if environment in ("test", "prod"):
            if not os.environ.get("MOTHERDUCK_TOKEN"):
                raise ValueError("MotherDuck token not found")
            conn = duckdb.connect(database='md:')
            try:
                conn.execute(f"CREATE DATABASE IF NOT EXISTS {self.database_name}")
                conn.execute(f"USE {self.database_name}")
            except duckdb.Error as e:
                raise RuntimeError(f"Failed to initialise MotherDuck database: {e}")
        else:
            try:
                conn = duckdb.connect(database=f"{self.database_name}")
            except duckdb.Error as e:
                raise RuntimeError(f"Failed to connect to local DuckDB database: {e}")
        return conn

    def setup_schema(self):
        """Set up schema and tables."""
        try:
            self.conn.execute(self.duckdb_schema)
        except duckdb.Error as e:
            raise RuntimeError(f"Failed to set up schema: {e}")

    def insert_data(self, table: pa.Table):
        """Insert data into the table."""
        try:
            self.conn.register("temp_table", table)
            insert_query = f"INSERT INTO {self.table_name} SELECT * FROM temp_table"
            self.conn.execute(insert_query)
            self.conn.unregister("temp_table")
        except duckdb.Error as e:
            raise RuntimeError(f"Failed to insert data into {self.table_name}: {e}")

    def close(self):
        """Close the database connection"""
        self.conn.close()

    def __repr__(self):
        return f"DuckDBDataIngestor(database_name={self.database_name}, table_name={self.table_name}, destination={self.destination})"

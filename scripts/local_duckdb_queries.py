import duckdb
from config import Config

settings = Config()

con = duckdb.connect(f"../{settings.database_name}")
con.sql("SELECT * FROM channel_stats_test").show()
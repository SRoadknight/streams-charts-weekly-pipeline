import duckdb
from config import Config

settings = Config()
csv_file = "../data/sample_data.csv"
con = duckdb.connect(database=f"../{settings.database_name}", read_only=False)

con.execute(f"""
CREATE OR REPLACE TABLE {settings.table_name} AS
SELECT * FROM read_csv('{csv_file}', header=True, delim=',')
""")

result = con.execute(f"SELECT * FROM {settings.table_name} LIMIT 5").fetchdf()
print(result)

con.close()
 


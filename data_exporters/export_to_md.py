from scripts.config import Config
import pyarrow as pa 
from scripts.db_operations import DuckDBDataIngestor
from scripts.test_utils import get_test_table_name 
if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter



@data_exporter
def export_data(df, *args, **kwargs):
    """
    Export data to MotherDuck
    """
    settings = Config()
    table_name = get_test_table_name(settings.table_name)

    # Log what mode we're running in
    print(f"Running in environment: {settings.ENVIRONMENT}")
    print(f"Using database: {settings.database_name}")
    print(f"Using table: {table_name}")

   
    

    pyarrow_schema = pa.schema([
        pa.field("platform", pa.string()),
        pa.field("channel_name", pa.string()),
        pa.field("channel_display_name", pa.string()),
        pa.field("channel_id", pa.string()),
        pa.field("hours_watched", pa.int64()),
        pa.field("peak_viewers", pa.int64()),
        pa.field("average_viewers", pa.int64()),
        pa.field("airtime_in_m", pa.int64()),
        pa.field("followers_gain", pa.int64()),
        pa.field("live_views", pa.string()),
        pa.field("last_streamed_game", pa.string()),
        pa.field("avatar_url", pa.string()),
        pa.field("channel_country", pa.string()),
        pa.field("stream_language", pa.string()),
        pa.field("partnership_status", pa.string()),
        pa.field("channel_type", pa.string()),
        pa.field("start_date", pa.date32()),  
        pa.field("end_date", pa.date32()),          
        pa.field("data_year", pa.int16()),           
        pa.field("week_number", pa.int8()),
        pa.field("rank_within_week", pa.int64()) 
    ])
    
    duckdb_schema_for_run = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        platform STRING, 
        channel_name STRING, 
        channel_display_name STRING, 
        channel_id STRING, 
        hours_watched INT64, 
        peak_viewers INT64, 
        average_viewers INT64, 
        airtime_in_m INT64, 
        followers_gain INT64, 
        live_views STRING, 
        last_streamed_game STRING, 
        avatar_url STRING, 
        channel_country STRING, 
        stream_language STRING, 
        partnership_status STRING, 
        channel_type STRING, 
        start_date DATE, 
        end_date DATE, 
        data_year INT16, 
        week_number INT8,
        rank_within_week BIGINT)
    """
    


    loader = DuckDBDataIngestor(
        duckdb_schema=duckdb_schema_for_run,
        pyarrow_schema=pyarrow_schema,
        database_name=settings.database_name,
        table_name=table_name,
        environment=settings.ENVIRONMENT
    )

    try:
        loader.setup_schema()
        arrow_table = pa.Table.from_pandas(df, schema=pyarrow_schema)
        loader.insert_data(arrow_table)
        print(f"Inserted {len(df)} rows into {table_name}")
    except Exception as e:
        raise RuntimeError("Failed to insert data: Database error")
    finally:
        if settings.ENVIRONMENT == "test":
            try:
                loader.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                print(f"Test table {table_name} dropped after test run.")
            except Exception as cleanup_error:
                print(f"Failed to drop test table {table_name}: {cleanup_error}")
        loader.close()

    


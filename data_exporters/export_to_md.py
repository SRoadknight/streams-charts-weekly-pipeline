from scripts.config import Config
import pyarrow as pa 
import os
from scripts.db_operations import DuckDBDataIngestor
if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter



@data_exporter
def export_data(df, *args, **kwargs):
    """
    Export data to MotherDuck
    """
    settings = Config()

    # Log what mode we're running in
    print(f"Running in environment: {settings.ENVIRONMENT}")
    print(f"Using database: {settings.database_name}")
    print(f"Using table: {settings.table_name}")

    # Log what would happen for GitHub Actions test mode
    if os.environ.get("GITHUB_ACTIONS_TEST") == "true":
        print(f"GITHUB_ACTIONS_TEST mode: Would export {len(df)} rows to {settings.database_name}.{settings.table_name}")
        return
    

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
    
    duckdb_schema = f"""
    CREATE TABLE IF NOT EXISTS {settings.table_name} (
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
        duckdb_schema=duckdb_schema,
        pyarrow_schema=pyarrow_schema,
        database_name=settings.database_name,
        table_name=settings.table_name,
        destination=settings.DESTINATION
    )

    try:
        loader.setup_schema()
        arrow_table = pa.Table.from_pandas(df, schema=pyarrow_schema)
        loader.insert_data(arrow_table)
    except Exception as e:
        raise RuntimeError("Failed to insert data: Database error")

    


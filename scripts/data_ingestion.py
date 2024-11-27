from config import settings
import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog
import datetime
import requests

def ingest_streams_data():
    try:
        url = "https://streamscharts.com/api/jazz/channels?platform=twitch&time=7-days"
        headers = {
            "Client-ID": settings.STREAMS_CHARTS_CLIENT_ID,
            "Token": settings.STREAMS_CHARTS_TOKEN
        }
        response = requests.get(url, headers=headers)


        data = response.json()
        df = pd.DataFrame(data['data'])

        start_date = datetime.datetime.today().date() - datetime.timedelta(days=datetime.datetime.today().weekday())
        end_date = start_date + datetime.timedelta(days=6)
        week_number = start_date.isocalendar()[1]
        year = start_date.year

        df['start_date'] = start_date
        df['end_date'] = end_date
        df['year'] = year
        df['week_number'] = week_number

        catalog = load_catalog(
            "glue",
            **{
                "type": "GLUE",
                "glue.region": settings.AWS_REGION,
            }
        )
        
        table = catalog.load_table(f"{settings.glue_database}.{settings.glue_table}")
        schema = table.schema().as_arrow()
        df = pa.Table.from_pylist(df.to_dict(orient="records"), schema=schema)
        table.append(df)

        return "Data ingestion completed successfully"
    except Exception as e:
        raise Exception(f"Ingestion failed: {str(e)}")
    
if __name__ == "__main__":
    ingest_streams_data()
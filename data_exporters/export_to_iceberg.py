from scripts.config import Config
from pyiceberg.catalog import load_catalog 
import pyarrow as pa 

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_data(df, *args, **kwargs):
    """
    Export data to Iceberg
    """
    settings = Config()
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
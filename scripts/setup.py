import json
import argparse
from pyiceberg.types import (
    StringType,
    NestedField,
    LongType,
    DateType
)
from pyiceberg.schema import Schema
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.catalog import load_catalog
from config import settings
from pathlib import Path

def load_terraform_outputs(env: str):
    tf_output_path = Path(f"infrastructure/terraform.output.{env}.json")
    with open(tf_output_path) as f:
        outputs = json.load(f)
        return {
            "bucket_name": outputs["s3_bucket_name"]["value"],
            "database_name": outputs["glue_database_name"]["value"]
        }

def create_iceberg_table(env: str):
    tf_outputs = load_terraform_outputs(env)
    
    # Simpler access to values
    bucket_name = tf_outputs["bucket_name"]
    database_name = tf_outputs["database_name"]


    catalog = load_catalog(
        "glue",
        **{
            "type": "GLUE",
            "glue.region": settings.AWS_REGION,
        }
    )


    schema = Schema(
        NestedField(field_id=1, name="platform", field_type=StringType(), required=False),
        NestedField(field_id=2, name="channel_name", field_type=StringType(), required=False),
        NestedField(field_id=3, name="channel_display_name", field_type=StringType(), required=False),
        NestedField(field_id=4, name="channel_id", field_type=StringType(), required=False),
        NestedField(field_id=5, name="hours_watched", field_type=LongType(), required=False),
        NestedField(field_id=6, name="peak_viewers", field_type=LongType(), required=False),
        NestedField(field_id=7, name="average_viewers", field_type=LongType(), required=False),
        NestedField(field_id=8, name="airtime_in_m", field_type=LongType(), required=False),
        NestedField(field_id=9, name="followers_gain", field_type=LongType(), required=False),
        NestedField(field_id=10, name="live_views", field_type=StringType(), required=False),
        NestedField(field_id=11, name="last_streamed_game", field_type=StringType(), required=False),
        NestedField(field_id=12, name="avatar_url", field_type=StringType(), required=False),
        NestedField(field_id=13, name="channel_country", field_type=StringType(), required=False),
        NestedField(field_id=14, name="stream_language", field_type=StringType(), required=False),
        NestedField(field_id=15, name="partnership_status", field_type=StringType(), required=False),
        NestedField(field_id=16, name="channel_type", field_type=StringType(), required=False),
        NestedField(field_id=17, name="start_date", field_type=DateType(), required=False),
        NestedField(field_id=18, name="end_date", field_type=DateType(), required=False),
        NestedField(field_id=19, name="year", field_type=LongType(), required=False),
        NestedField(field_id=20, name="week_number", field_type=LongType(), required=False)
    )

    partition_spec = PartitionSpec(
        PartitionField(source_id=19, field_id=1000, transform="identity", name="year"),
        PartitionField(source_id=20, field_id=1001, transform="identity", name="week_number")
    )

    table = catalog.create_table(
        schema=schema,
        partition_spec=partition_spec,
        location=f"s3://{bucket_name}/{settings.s3_prefix}",
        identifier=f"{database_name}.{settings.glue_table}"
    )

    print("Iceberg table created on Glue.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", choices=["dev", "prod"], required=True)
    args = parser.parse_args()
    create_iceberg_table(args.env)
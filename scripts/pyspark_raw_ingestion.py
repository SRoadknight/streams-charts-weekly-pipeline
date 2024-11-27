from pyiceberg.types import (
    TimestampType,
    StringType,
    NestedField,
    StructType,
    LongType,
    ListType
)
from pyiceberg.schema import Schema
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.catalog import load_catalog
from dotenv import load_dotenv
import os
import datetime
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

load_dotenv()

s3_bucket = os.getenv("S3_BUCKET")




# Define the structure for the `data` list elements (channel information)
data_element = StructType(
    NestedField(1, "airtime_in_m", LongType(), required=False),
    NestedField(2, "avatar_url", StringType(), required=False),
    NestedField(3, "average_viewers", LongType(), required=False),
    NestedField(4, "channel_country", StringType(), required=False),
    NestedField(5, "channel_display_name", StringType(), required=False),
    NestedField(6, "channel_id", StringType(), required=False),
    NestedField(7, "channel_name", StringType(), required=False),
    NestedField(8, "channel_type", StringType(), required=False),
    NestedField(9, "followers_gain", LongType(), required=False),
    NestedField(10, "hours_watched", LongType(), required=False),
    NestedField(11, "last_streamed_game", StringType(), required=False),
    NestedField(12, "live_views", StringType(), required=False),
    NestedField(13, "partnership_status", StringType(), required=False),
    NestedField(14, "peak_viewers", LongType(), required=False),
    NestedField(15, "platform", StringType(), required=False),
    NestedField(16, "stream_language", StringType(), required=False)
)

# Define the structure for `filters` field
filters = StructType(
    NestedField(1, "platform", StringType(), required=False),
    NestedField(2, "time", StringType(), required=False)
)

# Define the structure for `links` field
links = StructType(
    NestedField(1, "first", StringType(), required=False),
    NestedField(2, "last", StringType(), required=False),
    NestedField(3, "next", StringType(), required=False),
    NestedField(4, "prev", StringType(), required=False)
)

# Define the structure for `meta` field
meta = StructType(
    NestedField(1, "current_page", LongType(), required=False),
    NestedField(2, "from", LongType(), required=False),
    NestedField(3, "path", StringType(), required=False),
    NestedField(4, "per_page", LongType(), required=False),
    NestedField(5, "to", LongType(), required=False)
)

# Main schema definition
schema = Schema(
    NestedField(1, "data", ListType(element_type=data_element, element_id=1), required=False),  # List of data elements
    NestedField(2, "filters", filters, required=False),              # Filters struct
    NestedField(3, "links", links, required=False),                  # Links struct
    NestedField(4, "meta", meta, required=False),                    # Meta struct
    NestedField(5, "start_date", TimestampType(), required=True),
    NestedField(6, "end_date", TimestampType(), required=True),
    NestedField(7, "year", LongType(), required=True),               # `year` is required
    NestedField(8, "week_number", LongType(), required=True)         # `week_number` is required
)

catalog = load_catalog(
    "glue",
    **{
        "type": "GLUE",
        "glue.region": "eu-west-2",
    }
)

partition_spec = PartitionSpec(
    PartitionField(source_id=7, field_id=1000, transform="identity", name="year"),
    PartitionField(source_id=8, field_id=1001, transform="identity", name="week_number")
)

table = catalog.create_table(
    schema=schema,
    partition_spec=partition_spec,
    location=f"s3://{s3_bucket}/weekly_streamer_data/",
    identifier="weekly_stream_data.weekly_streamer_data"
)

print("Iceberg table created on Glue.")



# Load environment variables
load_dotenv()
client_id = os.getenv("STREAMS_CHARTS_CLINET_ID")
token = os.getenv("STREAMS_CHARTS_TOKEN")
s3_bucket = os.getenv("S3_BUCKET")

# API Request
url = "https://streamscharts.com/api/jazz/channels?platform=twitch&time=7-days"
headers = {"Client-ID": client_id, "Token": token}
response = requests.get(url, headers=headers)

if not response.status_code == 200:
    print(f"Failed to retrieve data: {response.status_code}")
    exit()

# Save JSON response to a local file (optional)
json_file_path = "weekly_streamer_data.json"
with open(json_file_path, "w") as file:
    file.write(response.text)

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("IcebergWithSparkAndGlue") \
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.type", "glue") \
    .config("spark.sql.catalog.glue_catalog.warehouse", f"s3a://{s3_bucket}/weekly_streamer_data/") \
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.defaultCatalog", "glue_catalog") \
    .getOrCreate()

# Read the JSON file into a Spark DataFrame
df_spark = spark.read.json(json_file_path)

# Add additional fields
start_date = datetime.datetime.today().date() - datetime.timedelta(days=datetime.datetime.today().weekday())
end_date = start_date + datetime.timedelta(days=6)
week_number = start_date.isocalendar()[1]
year = start_date.year

df_spark = df_spark \
    .withColumn("start_date", lit(str(start_date)).cast(TimestampType())) \
    .withColumn("end_date", lit(str(end_date)).cast(TimestampType())) \
    .withColumn("year", lit(year).cast(LongType())) \
    .withColumn("week_number", lit(week_number).cast(LongType()))

df_spark.printSchema()

# Define the target table in the Glue Catalog
database_name = "weekly_stream_data"
table_name = f"{database_name}.weekly_streamer_data"

# Write to Iceberg with partitioning
df_spark.write.format("iceberg") \
    .mode("append") \
    .partitionBy("year", "week_number") \
    .save(table_name)

# Verify the data was written
result = spark.sql(f"""
    SELECT 
        exploded_data.channel_name, 
        exploded_data.peak_viewers
    FROM glue_catalog.{database_name}.weekly_streamer_data
    LATERAL VIEW explode(data) AS exploded_data
""")
result.show()

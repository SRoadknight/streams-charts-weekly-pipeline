from dotenv import load_dotenv
import os 
import requests 
import pandas as pd
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

load_dotenv()

client_id = os.getenv("STREAMS_CHARTS_CLINET_ID")
token = os.getenv("STREAMS_CHARTS_TOKEN")
s3_bucket = os.getenv("S3_BUCKET")

url = "https://streamscharts.com/api/jazz/channels?platform=twitch&time=7-days"
headers = {"Client-ID": client_id, "Token": token}
response = requests.get(url, headers=headers)

if not response.status_code == 200:
    print(f"Failed to retrieve data: {response.status_code}")
    exit()

data = response.json()

json_file_path = "weekly_streamer_data.json"
with open(json_file_path, "w") as file:
    file.write(response.text)
    
df = pd.DataFrame(data['data'])

start_date = datetime.datetime.today().date() - datetime.timedelta(days=datetime.datetime.today().weekday())
end_date = start_date + datetime.timedelta(days=6)
week_number = start_date.isocalendar()[1]
year = start_date.year

df['start_date'] = start_date
df['end_date'] = end_date
df['year'] = year
df['week_number'] = week_number


df['start_date'] = pd.to_datetime(df['start_date'])
df['end_date'] = pd.to_datetime(df['end_date'])

# Setup Spark to work with Iceberg and Glue Catalog
spark = SparkSession.builder \
    .appName("IcebergWithSparkAndGlue") \
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.type", "glue") \
    .config("spark.sql.catalog.glue_catalog.warehouse", f"s3a://{s3_bucket}/weekly_streamer_data/") \
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.defaultCatalog", "glue_catalog") \
    .getOrCreate()


# Define the schema to match the Iceberg table schema
schema = StructType([
    StructField("platform", StringType(), True),
    StructField("channel_name", StringType(), True),
    StructField("channel_display_name", StringType(), True),
    StructField("channel_id", StringType(), True),
    StructField("hours_watched", LongType(), True),
    StructField("peak_viewers", LongType(), True),
    StructField("average_viewers", LongType(), True),
    StructField("airtime_in_m", LongType(), True),
    StructField("followers_gain", LongType(), True),
    StructField("live_views", StringType(), True),
    StructField("last_streamed_game", StringType(), True),
    StructField("avatar_url", StringType(), True),
    StructField("channel_country", StringType(), True),
    StructField("stream_language", StringType(), True),
    StructField("partnership_status", StringType(), True),
    StructField("channel_type", StringType(), True),
    StructField("start_date", TimestampType(), True),
    StructField("end_date", TimestampType(), True),
    StructField("year", LongType(), True),
    StructField("week_number", LongType(), True)
])


# Convert pandas DataFrame to Spark DataFrame
df_spark = spark.createDataFrame(df, schema=schema)


# Define the database and table names in the Glue Catalog
database_name = "weekly_stream_data"
table_name = f"{database_name}.weekly_streamer_data"


databases = spark.sql("SHOW DATABASES IN glue_catalog")
databases.show()

tables = spark.sql(f"SHOW TABLES IN glue_catalog.{database_name}")
tables.show()




# Working spark coommand:
"""
spark-submit --packages org.apache.iceberg:iceberg-spark3-runtime:0.12.0,
org.apache.hadoop:hadoop-aws:3.2.0,software.amazon.awssdk:url-connection-client:2.17.89,
software.amazon.awssdk:s3:2.17.89,
software.amazon.awssdk:glue:2.17.89 data_pull.py
"""

# Append data with partitioning by year and week_number
df_spark.write.format("iceberg") \
    .mode("append") \
    .partitionBy("year", "week_number") \
    .save(table_name)




"""
WORKING!
spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.0,org.apache.hadoop:hadoop-aws:3.2.0,software.amazon.awssdk:url-connection-client:2.20.0,software.amazon.awssdk:s3:2.29.9,software.amazon.awssdk:glue:2.20.0,software.amazon.awssdk:kms:2.20.0,software.amazon.awssdk:dynamodb:2.20.0,software.amazon.awssdk:sts:2.20.0 data_pull.py
"""

spark.sql("SHOW TABLES IN glue_catalog.weekly_stream_data").show()

"""
Also working with core added (Not sure if needed)
spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.0,org.apache.hadoop:hadoop-aws:3.2.0,software.amazon.awssdk:url-connection-client:2.29.9,software.amazon.awssdk:s3:2.29.9,software.amazon.awssdk:glue:2.29.9,software.amazon.awssdk:kms:2.29.9,software.amazon.awssdk:dynamodb:2.29.9,software.amazon.awssdk:sts:2.29.9,software.amazon.awssdk:core:2.29.9 data_pull.py
"""

# Query an Iceberg table (replace 'my_catalog' and 'my_table' with your actual catalog and table names)
result = spark.sql("SELECT channel_name, peak_viewers FROM glue_catalog.weekly_stream_data.weekly_streamer_data")
result.show()

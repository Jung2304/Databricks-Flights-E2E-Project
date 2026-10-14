from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

airports_rules = {
    "airport_id": "airport_id IS NOT NULL",
    "airport_name": "airport_name IS NOT NULL"
}

# Bronze: ingest the source delta table
@dp.view(name = "airports_stg")
def airports_stg():
    return (spark.readStream
            .format("delta")
            .load("/Volumes/flights_project/bronze/bronze_volume/airports/data/"))

# Silver preparation: transform and validate
@dp.expect_all_or_drop(airports_rules)
@dp.view(name = "airports_transform_view")
def airports_transform_view():
    df = (spark.readStream.table("airports_stg"))
    return (df.withColumn("modifiedDate", current_timestamp()))

# Create the SCD Type 2 target
dp.create_streaming_table(name = "flights_project.silver.airports_silver_table")

dp.create_auto_cdc_flow(
    target = "flights_project.silver.airports_silver_table",
    source = "airports_transform_view",
    keys = ["airport_id"],
    sequence_by = "modifiedDate",
    stored_as_scd_type = 1
)
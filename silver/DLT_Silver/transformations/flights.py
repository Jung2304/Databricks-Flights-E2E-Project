from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

flights_rules = {
    "flight_id": "flight_id IS NOT NULL"
}

# Bronze: ingest the source delta table
@dp.view(name = "flights_stg")
def flights_stg():
    return (spark.readStream
            .format("delta")
            .load("/Volumes/flights_project/bronze/bronze_volume/flights/data/"))

# Silver preparation: transform and validate
@dp.expect_all_or_drop(flights_rules)
@dp.view(name = "flights_transform_view")
def flights_transform_view():
    df = (spark.readStream.table("flights_stg"))
    return (df
            .withColumn("flight_date", to_date(col("flight_date"), "yyyy-MM-dd"))
            .withColumn("modifiedDate", current_timestamp()))

# Create the SCD Type 2 target
dp.create_streaming_table(name = "flights_project.silver.flights_silver_table")

dp.create_auto_cdc_flow(
    target = "flights_project.silver.flights_silver_table",
    source = "flights_transform_view",
    keys = ["flight_id"],
    sequence_by = "modifiedDate",
    stored_as_scd_type = 1
)
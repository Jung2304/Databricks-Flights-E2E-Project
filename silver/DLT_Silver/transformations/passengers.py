from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

passengers_rules = {
    "passenger_id": "passenger_id IS NOT NULL",
    "name": "name IS NOT NULL"
}

# Bronze: ingest the source delta table
@dp.view(name = "passengers_stg")
def passengers_stg():
    return (spark.readStream
            .format("delta")
            .load("/Volumes/flights_project/bronze/bronze_volume/passengers/data/"))

# Silver preparation: transform and validate
@dp.expect_all_or_drop(passengers_rules)
@dp.view(name = "passengers_transform_view")
def passengers_transform_view():
    df = (spark.readStream.table("passengers_stg"))
    return (df.withColumn("modifiedDate", current_timestamp()))

# Create the SCD Type 2 target
dp.create_streaming_table(name = "flights_project.silver.passengers_silver_table")

dp.create_auto_cdc_flow(
    target = "flights_project.silver.passengers_silver_table",
    source = "passengers_transform_view",
    keys = ["passenger_id"],
    sequence_by = "modifiedDate",
    stored_as_scd_type = 1
)
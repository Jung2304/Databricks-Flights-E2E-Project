from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

bookings_rules = {
    "booking_id": "booking_id IS NOT NULL",
    "passenger_id": "passenger_id IS NOT NULL",
    "flight_id": "flight_id IS NOT NULL",
    "airport_id": "airport_id IS NOT NULL",
    "booking_date": "booking_date IS NOT NULL",
    "valid_amount": "amount IS NOT NULL",
    "modifiedDate": "modifiedDate IS NOT NULL"
}

# Bronze: ingest the source Delta table
@dp.view(name = "bookings_stg")
def bookings_stg():
    return (spark.readStream
                .format('delta')
                .load("/Volumes/flights_project/bronze/bronze_volume/bookings/data/"))

# Silver preparation: transform and validate
@dp.expect_all_or_drop(bookings_rules)
@dp.view(name = "bookings_transform_view")
def bookings_transform_view():
    df = (spark.readStream.table("bookings_stg"))
    return (df
            .withColumn("amount", col("amount").cast(DoubleType()))
            .withColumn("booking_date", to_date(col("booking_date"), "yyyy-MM-dd"))
            .withColumn("modifiedDate", current_timestamp()))
    
# Create the SCD Type 2 target
dp.create_streaming_table(name = "flights_project.silver.bookings_silver_table")

dp.create_auto_cdc_flow(
    target = "flights_project.silver.bookings_silver_table",
    source = "bookings_transform_view",
    keys = ["booking_id"],
    sequence_by = "modifiedDate",
    stored_as_scd_type = 2
)
from pyspark import pipelines as dp
from pyspark.sql.functions import col, count, sum

# This file defines a sample transformation.
# Edit the sample below or add new transformations
# using "+ Add" in the file browser." adding comment"

@dp.table(
    comment="Aggregated NYC taxi trip data grouped by pickup zip code, with total trip count and total fare amount per zip.",
    table_properties={"layer": "silver"},
    cluster_by=["pickup_zip"]
)
def sample_aggregation_oct_6_1848():
    return (
        spark.read.table("samples.nyctaxi.trips")
        .groupBy(col("pickup_zip"))
        .agg(
            count("pickup_zip").alias("pickup_zip_count"),
            sum(col("fare_amount")).alias("total_fare_amount")
 
        )
    )

"""Bronze layer: raw UK Carbon Intensity API responses, read by Auto Loader.

One row per landing file. Auto Loader infers JSON fields as strings, so `data`
holds the array of half hours as JSON text; the silver layer parses and types it.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

LANDING_DIR = "/Volumes/portfolio/uk_carbon/landing"


@dp.table(
    name="uk_carbon_bronze.regional_raw",
    comment="Raw regional API responses, one row per landing file, all fields as strings",
)
def regional_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(LANDING_DIR)
        .select(
            "*",
            F.col("_metadata.file_path").alias("source_file"),
            F.col("_metadata.file_modification_time").alias("ingested_at"),
        )
    )
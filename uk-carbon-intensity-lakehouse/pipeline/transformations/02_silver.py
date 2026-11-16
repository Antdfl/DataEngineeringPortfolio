"""Silver layer: typed, filtered and deduplicated tables.

The bronze column `data` is parsed with an explicit schema (the contract with the API),
exploded to one row per half hour and distribution region, and deduplicated with AUTO CDC
on the key (region_id, period_start): the most recently written file wins.
All timestamps stay in UTC.
Data quality expectations run on the parsed rows, before AUTO CDC: four rules only warn,
the rule on the key stops the update (a NULL key means the API contract has changed).
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

API_SCHEMA = (
    "ARRAY<STRUCT<`from`: STRING, `to`: STRING, "
    "regions: ARRAY<STRUCT<regionid: INT, dnoregion: STRING, shortname: STRING, "
    "intensity: STRUCT<forecast: INT, index: STRING>, "
    "generationmix: ARRAY<STRUCT<fuel: STRING, perc: DOUBLE>>>>>>"
)
API_TIME_FORMAT = "yyyy-MM-dd'T'HH:mmX"

INTENSITY_WARNINGS = {
    "forecast_not_null": "forecast_gco2_kwh IS NOT NULL",
    "forecast_not_negative": "forecast_gco2_kwh >= 0",
    "index_known": "intensity_index IN ('very low', 'low', 'moderate', 'high', 'very high')",
    "half_hour_period": "period_end = period_start + INTERVAL 30 MINUTES",
}


@dp.view(comment="One row per half hour and distribution region (ids 1 to 14), duplicates included")
@dp.expect_all(INTENSITY_WARNINGS)
@dp.expect_or_fail("key_not_null", "period_start IS NOT NULL")
def regional_half_hours():
    return (
        spark.readStream.table("uk_carbon_bronze.regional_raw")
        .select("source_file", "ingested_at", F.explode(F.from_json("data", API_SCHEMA)).alias("hh"))
        .select(
            "source_file",
            "ingested_at",
            F.col("hh.from").alias("from_text"),
            F.col("hh.to").alias("to_text"),
            F.explode("hh.regions").alias("r"),
        )
        .where(F.col("r.regionid").between(1, 14))
        .select(
            F.col("r.regionid").alias("region_id"),
            F.to_timestamp("from_text", API_TIME_FORMAT).alias("period_start"),
            F.to_timestamp("to_text", API_TIME_FORMAT).alias("period_end"),
            F.col("r.intensity.forecast").alias("forecast_gco2_kwh"),
            F.col("r.intensity.index").alias("intensity_index"),
            "source_file",
            "ingested_at",
        )
    )


dp.create_streaming_table(
    name="uk_carbon_silver.intensity",
    comment="Carbon intensity forecast, one row per region and half hour (UTC); the latest file wins",
)

dp.create_auto_cdc_flow(
    target="uk_carbon_silver.intensity",
    source="regional_half_hours",
    keys=["region_id", "period_start"],
    sequence_by="ingested_at",
    stored_as_scd_type=1,
)
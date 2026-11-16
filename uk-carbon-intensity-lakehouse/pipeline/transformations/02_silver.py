"""Silver layer: typed, filtered and deduplicated tables.

The bronze column `data` is parsed with an explicit schema (the contract with the API),
exploded to one row per half hour and distribution region, and deduplicated with AUTO CDC:
the most recently written file wins. One table per grain:
- intensity: one row per region and half hour, key (region_id, period_start);
- generation_mix: one row per region, half hour and fuel, key (region_id, period_start, fuel).
All timestamps stay in UTC.
The parsing lives in one function shared by the two views.
Data quality expectations run on the views, before AUTO CDC: the rules on values only warn,
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

MIX_WARNINGS = {
    "percentage_not_null": "percentage IS NOT NULL",
    "percentage_in_range": "percentage BETWEEN 0 AND 100",
    "fuel_known": (
        "fuel IN ('biomass', 'coal', 'gas', 'hydro', 'imports', "
        "'nuclear', 'other', 'solar', 'wind')"
    ),
}


def parse_regional_half_hours():
    """Contract with the API: parse, explode, keep regions 1 to 14, convert UTC timestamps.

    A plain function, not a dataset: each view calls it and keeps its own columns.
    The struct `r` carries the region details still to be selected.
    """
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
            "r",
            "source_file",
            "ingested_at",
        )
    )


@dp.view(comment="One row per half hour and distribution region (ids 1 to 14), duplicates included")
@dp.expect_all(INTENSITY_WARNINGS)
@dp.expect_or_fail("key_not_null", "period_start IS NOT NULL")
def regional_half_hours():
    return parse_regional_half_hours().select(
        "region_id",
        "period_start",
        "period_end",
        F.col("r.intensity.forecast").alias("forecast_gco2_kwh"),
        F.col("r.intensity.index").alias("intensity_index"),
        "source_file",
        "ingested_at",
    )


@dp.view(comment="One row per half hour, distribution region and fuel, duplicates included")
@dp.expect_all(MIX_WARNINGS)
@dp.expect_or_fail("key_not_null", "period_start IS NOT NULL AND fuel IS NOT NULL")
def regional_fuel_mix():
    return (
        parse_regional_half_hours()
        .select("region_id", "period_start", F.explode("r.generationmix").alias("m"), "source_file", "ingested_at")
        .select(
            "region_id",
            "period_start",
            F.col("m.fuel").alias("fuel"),
            F.col("m.perc").alias("percentage"),
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

dp.create_streaming_table(
    name="uk_carbon_silver.generation_mix",
    comment="Generation mix in percent, one row per region, half hour (UTC) and fuel; the latest file wins",
)

dp.create_auto_cdc_flow(
    target="uk_carbon_silver.generation_mix",
    source="regional_fuel_mix",
    keys=["region_id", "period_start", "fuel"],
    sequence_by="ingested_at",
    stored_as_scd_type=1,
)
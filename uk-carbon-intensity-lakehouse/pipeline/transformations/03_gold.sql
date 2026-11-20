-- Gold layer: wide, fine-grained views for the metric views, the dashboard and Genie.
-- The grain stays the same as the silver and no averages are stored here: the measures live in the metric views.
-- Timestamps stay in UTC and are also shown in London clock time, the time zone of the people who use the data.
-- convert_timezone with two arguments gives the right London time with any session time zone.

CREATE OR REFRESH MATERIALIZED VIEW uk_carbon_gold.mv_intensity_half_hour (
  CONSTRAINT region_name_known EXPECT (region_name IS NOT NULL)
)
COMMENT "Carbon intensity forecast, one row per region and half hour, with London clock time"
TBLPROPERTIES ('delta.feature.timestampNtz' = 'supported')
AS
WITH uk AS (
  SELECT i.region_id,
         r.short_name                                       AS region_name,
         i.period_start                                     AS period_start_utc,
         convert_timezone('Europe/London', i.period_start)  AS period_start_uk,
         i.forecast_gco2_kwh,
         i.intensity_index
  FROM uk_carbon_silver.intensity i
  LEFT JOIN uk_carbon_silver.regions r
         ON i.region_id = r.region_id
)
SELECT region_id,
       region_name,
       period_start_utc,
       period_start_uk,
       to_date(period_start_uk)                             AS date_uk,
       hour(period_start_uk)                                AS hour_uk,
       extract(DAYOFWEEK_ISO FROM to_date(period_start_uk)) AS day_of_week_uk,
       date_format(to_date(period_start_uk), 'E')           AS day_name_uk,
       forecast_gco2_kwh,
       intensity_index
FROM uk;

-- Fuel groups answer "does the source replenish itself?", not "how much CO2 does it emit?":
-- CO2 is already measured by forecast_gco2_kwh in mv_intensity_half_hour.
-- Biomass is Renewable, as in NESO's annual review; nuclear emits no CO2 while generating but is not renewable.
-- No ELSE in the CASE: an unknown fuel gets NULL and the fuel_group_known expectation shows it in the event log.

CREATE OR REFRESH MATERIALIZED VIEW uk_carbon_gold.mv_fuel_mix_half_hour (
  CONSTRAINT region_name_known EXPECT (region_name IS NOT NULL),
  CONSTRAINT fuel_group_known  EXPECT (fuel_group IS NOT NULL)
)
COMMENT "Generation mix in percent, one row per region, half hour and fuel, with fuel group and London clock time"
TBLPROPERTIES ('delta.feature.timestampNtz' = 'supported')
AS
WITH uk AS (
  SELECT g.region_id,
         r.short_name                                       AS region_name,
         g.period_start                                     AS period_start_utc,
         convert_timezone('Europe/London', g.period_start)  AS period_start_uk,
         g.fuel,
         g.percentage
  FROM uk_carbon_silver.generation_mix g
  LEFT JOIN uk_carbon_silver.regions r
         ON g.region_id = r.region_id
)
SELECT region_id,
       region_name,
       period_start_utc,
       period_start_uk,
       to_date(period_start_uk)                             AS date_uk,
       hour(period_start_uk)                                AS hour_uk,
       extract(DAYOFWEEK_ISO FROM to_date(period_start_uk)) AS day_of_week_uk,
       date_format(to_date(period_start_uk), 'E')           AS day_name_uk,
       fuel,
       CASE
         WHEN fuel IN ('wind', 'solar', 'hydro', 'biomass') THEN 'Renewable'
         WHEN fuel IN ('gas', 'coal')                       THEN 'Fossil'
         WHEN fuel IN ('nuclear', 'imports', 'other')       THEN 'Nuclear and other'
       END                                                  AS fuel_group,
       percentage
FROM uk;

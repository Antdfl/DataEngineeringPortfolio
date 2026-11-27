# UK Carbon Intensity Lakehouse: design document

Status: draft, sections 1 to 9. Last updated: 2026-11-22.

## 1. Data source

### 1.1 Overview

| Item | Value |
|---|---|
| Provider | National Energy System Operator (NESO), Great Britain |
| API | Carbon Intensity API v2.0.0, `https://api.carbonintensity.org.uk` |
| Endpoint used for ingestion | `GET /regional/intensity/{from}/{to}` (all regions, no `regionid` filter) |
| Authentication | None |
| Granularity | 30 minutes, timestamps in UTC |
| Content | Regional carbon intensity `forecast` (gCO2/kWh), intensity `index`, generation mix (9 fuels, percentages) |
| Documentation | https://carbon-intensity.github.io/api-definitions/ |
| Terms of use | https://github.com/carbon-intensity/terms |

### 1.2 Licence, terms and attribution

* The API documentation states the licence as CC BY 4.0. In the `terms` repository, CC BY 4.0 is linked as the licence of the terms document itself, while API usage is granted under the Terms of Use. This project cites both.
* Attribution used in this project (text only, no logo): *Data: Carbon Intensity API, National Energy System Operator (NESO), CC BY 4.0.*
* Relevant restrictions: no use of the NESO name, brand or logo without written permission; no claim of endorsement by NESO; no service that replaces the NESO service; no resale or sublicensing of access.
* The API is provided "as is" and may change or be suspended without notice. The terms may also change without notice (governing law: United Kingdom).
* NESO applies a rate limit, but its value is not published.

This is a portfolio project with no commercial use. Nothing in this section is legal advice.

### 1.3 Measured behaviour

The following behaviour was measured from a Databricks Free Edition notebook on 2026-10-24, 2026-11-07 and 2026-11-08. Where the documentation is silent or different, the measured behaviour wins.

| Aspect | Documentation | Measured |
|---|---|---|
| Max range, regional endpoint **with** `regionid` | not stated (14 days is stated for the national endpoint) | 60 days |
| Max range, regional endpoint **without** `regionid` | not stated | 14 days |
| How the range is counted | not stated | On the widened window (see next row): `14 days minus 30 min` is accepted, `14 days` is rejected; same pattern at 60 days |
| Rows returned | not stated | One extra half hour at the start: the half hour that **ends** at the requested start |
| Range too long | not stated | HTTP 400, JSON body containing `\r` characters, returned in under 0.5 s |
| No data for the requested period | not stated | HTTP 200 with zero rows |
| Start of regional history | not stated | Between 2018-05-01 and 2018-06-01 |
| Regions per half hour | not stated | 18 (ids 1 to 18) |
| Fuels per region and half hour | not stated | 9, always present, also when 0 |
| Sum of the generation mix | not stated | Between 99.7 and 100.3 over the whole history (rounding), checked on 2026-11-16 |
| Regional data | flagged as beta | Consistent on the day checked; the 24-month backfill showed the two anomalies below |
| Multi-day range crossing 31 December | not stated | The response ends with the half hour starting at 23:00 UTC on 31 December; the rest of the range is missing. Seen at the end of 2024 and of 2025, for all regions. One-day ranges return the missing data. Cause unknown |
| Gaps in the source | not stated | Two gaps, identical for all regions and for any range length: 17 half hours starting from 2025-01-12T23:00Z to 2025-01-13T07:00Z, and 8 half hours starting from 2025-08-10T23:00Z to 2025-08-11T02:30Z. Not recoverable as of 2026-11-08 |

Response size and time for the endpoint without `regionid` (2026-11-07):

| Requested range | Half hours returned | Size | Time |
|---|---|---|---|
| 7 days | 337 (336 + 1) | 2.2 MiB | 2.6 s |
| 13 days | 625 (624 + 1) | 4.1 MiB | 3.6 s |
| 14 days minus 30 min | 672 (671 + 1) | 4.5 MiB | 3.5 s |
| 14 days | HTTP 400 | 164 bytes | 0.3 s |

### 1.4 Regions

* Ids 1 to 14 are distribution network areas (for example 13 = London). Ids 15 to 18 are aggregates (England, Scotland, Wales, GB), like subtotal rows of a `GROUP BY ROLLUP`. **Only ids 1 to 14 are used**, so that regions are comparable and nothing is counted twice.
* Regions are electricity distribution areas, not administrative boundaries.
* The API does not provide regional electricity demand.
* Each id had one name (`shortname`, `dnoregion`) over the whole history, checked on 2026-11-17. The operator names are not up to date: for example the API still uses `WPD`, although Western Power Distribution was renamed National Grid Electricity Distribution in September 2022.

### 1.5 Design consequences

| Measured fact | Design decision |
|---|---|
| Endpoint without `regionid` returns all regions in one call | One call per time block instead of 14 |
| 14-day limit on that endpoint | Backfill in **7-day blocks**, from 00:00 UTC to 00:00 UTC: half the limit as safety margin, and readable file names. 106 calls for the 24-month backfill |
| One extra half hour at the start of every response | The half hour at each block boundary arrives twice. Deduplication on the key (region, half-hour start) in the silver layer |
| Ids 15 to 18 are aggregates | Silver keeps only ids 1 to 14: a project filter in the parsing function, not a data quality rule (see 4.3) |
| HTTP 200 with zero rows is possible | The ingestion log records the number of half hours received for every call |
| A call can return fewer half hours than expected (year end, source gaps) | The ingestion log stores the expected half hours and a completeness flag for every call; incomplete calls are flagged, not blocked |
| Rate limit not published | Pause between calls; retries with exponential backoff on 429 and 5xx; HTTP 400 stops the task without retries |
| API may change without notice | The raw response is stored unchanged, so the silver layer can be rebuilt without calling the API again |
| Multi-day ranges lose data after 23:00 UTC on 31 December | The days around each year end are downloaded again with one-day ranges (14 extra files in the backfill). The 7-day files are kept as a record; the overlap is removed by the silver deduplication |
| Two gaps in the source (25 half hours per region) | Documented as a known limitation. The completeness check (48 half hours per region and UTC day) is a query run after the pipeline, not a blocking rule (see 4.5) |

## 2. Architecture

### 2.1 Layers and schemas

The project uses one Unity Catalog schema per layer, all in the `portfolio` catalog. The schemas are created by `setup/00_create_catalog_schema_volume` and `setup/02_create_layer_schemas`.

| Schema | Layer | Content |
|---|---|---|
| `portfolio.uk_carbon` | Ingestion | Landing volume (raw API responses as JSON files) and ingestion log |
| `portfolio.uk_carbon_bronze` | Bronze | Raw API responses read by Auto Loader, all fields as strings |
| `portfolio.uk_carbon_silver` | Silver | Typed, filtered and deduplicated tables |
| `portfolio.uk_carbon_gold` | Gold | Materialized views built by the pipeline and metric views for the dashboard (see 5) |

Table names do not repeat the layer, because the schema already says it (for example `uk_carbon_bronze.regional_raw`). Code always uses `schema.table`, also for the default schema of the pipeline.

### 2.2 Data flow

```
API -> ingestion notebook -> landing volume -> bronze -> silver -> gold -> dashboard
               |
               v
         ingestion log
```

* **Area 1, ingestion** (`ingestion/01_ingest_regional_api`): a notebook calls the API, writes one JSON file per call to the landing volume and one row per call to the ingestion log.
* **Area 2, pipeline** (`pipeline/transformations/`): one Lakeflow Spark Declarative Pipeline builds bronze, silver and gold. The landing files are the contract between the two areas.

## 3. Bronze layer

### 3.1 Table `uk_carbon_bronze.regional_raw`

Streaming table defined in `pipeline/transformations/01_bronze.py`, read from the landing volume by Auto Loader (`cloudFiles`, format JSON). **One row per landing file.**

| Column | Type | Source |
|---|---|---|
| `data` | STRING | The `data` field of the API response: the whole array of half hours, as JSON text |
| `_rescued_data` | STRING | Added by Auto Loader: fields that do not match the inferred schema |
| `source_file` | STRING | `_metadata.file_path` |
| `ingested_at` | TIMESTAMP | `_metadata.file_modification_time`: when the file was written |

### 3.2 Design decisions

| Decision | Reason |
|---|---|
| Faithful copy of the files: no filter, no deduplication, no quality rule | If a rule changes, only the silver transformation changes and gold is recomputed; the API is not called again |
| All fields as strings (no `cloudFiles.inferColumnTypes`) | The bronze layer cannot fail on a data type. Types are set in one place, in the silver layer, with `from_json` and an explicit schema |
| Duplicates kept | Auto Loader processes each file once (it recognises files by path, not by content), but overlapping calls bring the same half hours again. Deduplication is done in the silver layer with AUTO CDC on the key (region, half-hour start) |
| `ingested_at` from the file modification time, not `current_timestamp()` | After a full refresh, the read time would be almost the same for every file. The write time orders the versions of a half hour: it is the `sequence_by` column for AUTO CDC |
| `_rescued_data` kept | Faithful copy; a non-empty value signals a change in the API format |

### 3.3 Checks

Checked on 2026-11-14 and 2026-11-15 with SQL queries: bronze rows equal to the files in the ingestion log, the four columns above, no rescued data, and the number of half hours equal to the log file by file (`json_array_length(data)` against `n_half_hours`). A second run with no new files wrote no records.

## 4. Silver layer

### 4.1 Tables

Defined in `pipeline/transformations/02_silver.py`. One table per grain, all in `portfolio.uk_carbon_silver`; all timestamps stay in UTC.

| Table | Grain | Key | Other columns | Rows on 2026-11-17 |
|---|---|---|---|---|
| `intensity` | Region and half hour | `region_id`, `period_start` | `period_end`, `forecast_gco2_kwh`, `intensity_index`, `source_file`, `ingested_at` | 501,452 |
| `generation_mix` | Region, half hour and fuel | `region_id`, `period_start`, `fuel` | `percentage`, `source_file`, `ingested_at` | 4,513,068 |
| `regions` | Region | `region_id` | `short_name`, `dno_region`, `source_file`, `ingested_at` | 14 |

`source_file` and `ingested_at` record the landing file that each row comes from.

### 4.2 Data flow

```
uk_carbon_bronze.regional_raw
        |
        v
parse_regional_half_hours()     Python function, not a dataset
        |
        +--> view regional_half_hours (5 rules) --AUTO CDC--> intensity
        +--> view regional_fuel_mix   (4 rules) --AUTO CDC--> generation_mix
        +--> view regional_names      (3 rules) --AUTO CDC--> regions
```

The function parses the JSON text with an explicit schema, explodes it to one row per half hour and region, keeps ids 1 to 14 and converts the timestamps. It returns the region struct `r` unchanged, so each view selects only the columns of its own grain; the mix view explodes one more level, the fuels. In SQL terms: one common table expression and three `SELECT` statements. Because the function is code and not a dataset, it does not appear in the pipeline graph: each view runs it on its own stream, with its own checkpoint.

### 4.3 Design decisions

| Decision | Reason |
|---|---|
| `from_json` with a schema written in the code | The schema is the contract with the API, kept in one place. `from_json` does not fail on a wrong type: the field becomes NULL, and the expectations count it |
| Ids 1 to 14 kept by a filter in the parsing function | A project choice (see 1.4), not a data quality rule: the rows of ids 15 to 18 are correct, they are just not used |
| One table per grain | Each fact is stored once, at its own level of detail; the gold layer joins the tables |
| AUTO CDC, SCD type 1, `sequence_by` = `ingested_at` | A declarative `MERGE`: the most recently written file wins, also when an older file is processed later. Before merging, AUTO CDC keeps one row per key, so the duplicates from overlapping files do not make the merge fail |
| `regions`: `sequence_by` = `struct(ingested_at, period_start)` | In one file a region appears once per half hour, always with the same `ingested_at`: a tie. AUTO CDC needs one sequencing value per key, so `period_start` breaks the tie. It is used for ordering only and is not stored |
| `regions` as SCD type 1 | The dashboard needs the current names, and the names never changed in the history (see 1.4). SCD type 2 would keep the history of names if they started to change |
| Expectations on the views, before AUTO CDC | The rules run where NULLs are created (`from_json`, `to_timestamp`), and a NULL key is stopped before the merge. They count the rows arriving from the view, duplicates included |

### 4.4 Data quality rules (expectations)

Rules that only warn keep the row and count it in the pipeline event log. Rules that fail stop the update: a NULL key or sequence means that the API contract has changed, and the job failure email is the alarm.

| View | Rule | Condition | Action |
|---|---|---|---|
| `regional_half_hours` | `forecast_not_null` | `forecast_gco2_kwh IS NOT NULL` | Warn |
| | `forecast_not_negative` | `forecast_gco2_kwh >= 0` (0 occurs) | Warn |
| | `index_known` | `intensity_index` in the five known values | Warn |
| | `half_hour_period` | `period_end = period_start + INTERVAL 30 MINUTES` | Warn |
| | `key_not_null` | `period_start IS NOT NULL` | Fail |
| `regional_fuel_mix` | `percentage_not_null` | `percentage IS NOT NULL` | Warn |
| | `percentage_in_range` | `percentage BETWEEN 0 AND 100` (0 and 100 occur) | Warn |
| | `fuel_known` | `fuel` in the nine known fuels | Warn |
| | `key_not_null` | `period_start IS NOT NULL AND fuel IS NOT NULL` | Fail |
| `regional_names` | `dno_region_not_null` | `dno_region IS NOT NULL` | Warn |
| | `short_name_not_null` | `short_name IS NOT NULL` | Warn |
| | `sequence_not_null` | `period_start IS NOT NULL` | Fail |

On the whole history no rule was violated: 512,218 rows checked for `intensity` and 4,609,962 for the mix on 2026-11-16, 513,576 for the names on 2026-11-17.

### 4.5 Aggregate checks

An expectation looks at one row at a time. Rules on groups of rows are SQL queries, run after the pipeline:

| Check | Expected | Result on 2026-11-17 |
|---|---|---|
| Unique keys | Rows equal to distinct keys | True for the three tables |
| Completeness: 48 half hours per region and UTC day | Only known exceptions | Six days: the first day (2024-10-31, 1 half hour), the two source gaps (see 1.3; 2025-01-12 and 13: 46 and 33; 2025-08-10 and 11: 46 and 42) and the current day |
| Generation mix: 9 fuels per region and half hour | `generation_mix` rows = 9 x `intensity` rows | 4,513,068 = 9 x 501,452 |
| Sum of the generation mix per region and half hour | 100 +/- 0.5 | No value outside; from 99.7 to 100.3 |

### 4.6 Incremental processing

A daily run reads only the new landing file. The Delta history of `intensity` after the run of 2026-11-17 shows a `MERGE` with 1,358 source rows (97 half hours x 14 regions), 672 rows inserted (48 new half hours), 686 updated (the 49 half hours already present) and 0 deleted. A full refresh of the same table reads the whole bronze table again: on 2026-11-16 its `MERGE` received 500,780 rows, one per key after deduplication, and inserted all of them.

## 5. Gold layer

### 5.1 Approach

The gold layer has two parts:

* **Materialized views** in the pipeline (`pipeline/transformations/03_gold.sql`): wide tables at the same fine grain as the silver tables, with the joins, labels and calendar columns that the dashboard needs. They store no averages.
* **Metric views** on top of them (section 8): they hold the measures (the formulas), and whoever queries them chooses the grouping. Every consumer of a metric view gets the same formulas.

| Option | Outcome | Reason |
|---|---|---|
| Metric views directly on the silver tables | Rejected | No gold layer in the medallion design; all the logic would live in YAML |
| **Fine-grained materialized views, with metric views on top** | **Chosen** | One orchestration (same pipeline, same job), plain SQL, one set of formulas for every consumer |
| The same, with materialized metric views | Rejected | Experimental feature, no gain at about 0.5 and 4.5 million rows. A possible future step |
| Pre-aggregated gold tables, no metric views | Rejected | Rigid, and an average of averages is wrong when the groups have different sizes |
| Tables written by a notebook | Rejected | More code, and the gold layer would be outside the pipeline graph |

### 5.2 Materialized views

All in `portfolio.uk_carbon_gold`, defined in `pipeline/transformations/03_gold.sql`.

| View | Grain | Sources | Rows checked on 2026-11-20 |
|---|---|---|---|
| `mv_intensity_half_hour` | Region and half hour | `intensity`, `regions` | 502,796 |
| `mv_fuel_mix_half_hour` | Region, half hour and fuel | `generation_mix`, `regions` | 4,525,164 |

Columns shared by the two views:

| Column | Type | Content |
|---|---|---|
| `region_id` | INT | Region id (1 to 14) |
| `region_name` | STRING | `short_name` from `regions`, for example London |
| `period_start_utc` | TIMESTAMP | Start of the half hour, an instant (UTC) |
| `period_start_uk` | TIMESTAMP_NTZ | The same start as shown by a clock in London |
| `date_uk` | DATE | London date: days are cut at London midnight |
| `hour_uk` | INT | London hour, 0 to 23 |
| `day_of_week_uk` | INT | ISO day of the week, Monday 1 to Sunday 7: sorts the days |
| `day_name_uk` | STRING | `Mon` to `Sun`: labels the days |

Columns of each view:

| View | Columns |
|---|---|
| `mv_intensity_half_hour` | `forecast_gco2_kwh`, `intensity_index` |
| `mv_fuel_mix_half_hour` | `fuel`, `fuel_group` (see 5.4), `percentage` |

Not copied from the silver tables: `period_end` (always `period_start` plus 30 minutes), `source_file` and `ingested_at` (lineage, kept in silver), `dno_region` (operator names, partly out of date, see 1.4).

### 5.3 Design decisions

| Decision | Reason |
|---|---|
| Names: `mv_` for materialized views, `metrics_` for metric views | The type of each object is visible in its name |
| London clock time next to UTC | The data describe the British grid and British habits. In UTC the evening peak would move by one hour between summer and winter. Rule: store in UTC, present in the time zone of the people who use the data |
| `convert_timezone('Europe/London', period_start)`, two arguments | With two arguments the source time zone is the session one, so the result is right with any session time zone (tested with a session in Europe/Rome). The three-argument form with `'UTC'` is right only when the session is in UTC |
| Day of the week as a number and a name | A name alone sorts alphabetically (Fri, Mon, Sat, ...) |
| `LEFT JOIN` with `regions` plus an expectation | An inner join would drop rows without a name silently; here the row stays and the event log counts it |
| `TBLPROPERTIES ('delta.feature.timestampNtz' = 'supported')` in the definition | Delta tables declare the features they use, and `TIMESTAMP_NTZ` must be enabled explicitly: the first run failed with `DELTA_FEATURES_REQUIRE_MANUAL_ENABLEMENT`. The property is in the code, not set with a manual `ALTER TABLE`, because the pipeline runs the definition again at every update and only the code in the repository survives a rebuild |
| Two views, one per grain | Intensity and mix are master and detail. Joining them would repeat each forecast for the nine fuels, and a sum of the forecast would be nine times too large (fan-out). The views meet in the dashboard through their common columns: region, date and hour |

### 5.4 Fuel groups

The nine fuels of the API are grouped into three groups, to keep the generation mix readable in a chart. The groups answer the question "does the source replenish itself?", not "how much CO2 does it emit?": CO2 is already measured by `forecast_gco2_kwh` in `mv_intensity_half_hour`.

| `fuel_group` | Fuels | Note |
|---|---|---|
| `Renewable` | wind, solar, hydro, biomass | Biomass is counted as renewable, as in NESO's annual review, because the trees are replanted and grow again. It is not zero carbon: the API uses a factor of 120 gCO2/kWh for it |
| `Fossil` | gas, coal | |
| `Nuclear and other` | nuclear, imports, other | Nuclear emits no CO2 while generating (factor 0 in the API) but is not renewable. Imports have a variable carbon intensity, calculated from the generation mix of the exporting country |

The `CASE` that sets the group has no `ELSE`: a fuel added by the API would get a NULL group, and the `fuel_group_known` expectation would show it in the event log.

### 5.5 Data quality rules (expectations)

All the gold rules only warn: the row is kept and counted in the event log.

| View | Rule | Condition |
|---|---|---|
| `mv_intensity_half_hour` | `region_name_known` | `region_name IS NOT NULL` |
| `mv_fuel_mix_half_hour` | `region_name_known` | `region_name IS NOT NULL` |
| | `fuel_group_known` | `fuel_group IS NOT NULL` |

No rule was violated on the first runs (2026-11-19 and 2026-11-20).

### 5.6 Checks

| Check | Expected | Result |
|---|---|---|
| Rows and keys | Rows equal to the distinct keys and to the rows of the silver source | 502,796 and 4,525,164: true for both views |
| Region names | 14 names, no NULL | True for both views |
| London days with a clock change (`mv_intensity_half_hour`) | 46 half hours per region on the last Sunday of March, 50 on the last Sunday of October, 48 on a normal day | 2025-03-30: 46, 2025-10-26: 50, 2026-03-29: 46, 2026-10-25: 50, 2026-11-18: 48 |
| London rows with `hour_uk` = 1 | 2 on a normal day, 4 on 2026-10-25, none on 2026-03-29 | As expected |
| Fuel groups | Each fuel in one group; rows = 502,796 x the fuels of the group | Fossil 1,005,592; Nuclear and other 1,508,388; Renewable 2,011,184 |
| Sum of the generation mix per region and half hour | 100 +/- 0.5 | No value outside; from 99.7 to 100.3 |

The clock-change days are the proof that the gold layer cuts days in London time: in UTC every day has 48 half hours (see 4.5).

### 5.7 Refresh

A materialized view added to the pipeline does not need a full refresh: the first normal run computes it completely ("Full recompute" on the pipeline page). On 2026-11-20 a run with no new data showed "No change" for `mv_intensity_half_hour`: the sources had not changed, so nothing was recomputed. On the same day, a run with new data showed "Incremental" for the gold views. The number of rows recomputed by an incremental refresh has not been measured.

## 6. Pipeline `uk_carbon_pipeline`

The pipeline settings live in the workspace, not in the repository. They are recorded here so that the project can be rebuilt. Databricks Asset Bundles are a possible future step.

| Setting | Value |
|---|---|
| Name | `uk_carbon_pipeline` |
| Type | ETL pipeline, created with "Add existing assets" |
| Pipeline mode | Triggered |
| Compute | Serverless |
| Root folder | `uk-carbon-intensity-lakehouse/pipeline` (in the Git folder) |
| Source code | The folder `transformations/` |
| Default catalog and schema | `portfolio`, `uk_carbon_bronze` |

Source files are plain Python or SQL files, not notebooks; the pipeline chooses the language from the file extension. They are numbered (`01_bronze.py`, `02_silver.py`, `03_gold.sql`) for readers; the execution order is decided by the pipeline graph.

## 7. Orchestration: job `uk_carbon_refresh`

### 7.1 Tasks

| Task | Type | Settings | Retries |
|---|---|---|---|
| `ingest_update` | Notebook `ingestion/01_ingest_regional_api` from the Git folder, serverless | Parameters `mode` = `update`, `lookback_hours` = `48` | 2 retries (3 attempts in total), 10 minutes apart; serverless auto-optimization disabled |
| `refresh_pipeline` | Pipeline `uk_carbon_pipeline`, no full refresh | Depends on `ingest_update`; runs only if it succeeded | 1 retry (2 attempts in total), 5 minutes apart |
| `daily_checks` | Notebook `checks/01_daily_job_checks` from the Git folder, serverless | Depends on `refresh_pipeline` | None (a check in `KO` does not recover by trying again) |

### 7.2 Job settings

| Setting | Value | Reason |
|---|---|---|
| Schedule | Every day at 18:10, Europe/Rome | One file per day is enough for this project |
| Update window | The last 48 hours, ending at the current time rounded down to the half hour (2 x 48 + 1 = 97 half hours, see 1.3) | The window is twice the interval: if one day fails, the next run fills the gap. The overlap is removed by the silver deduplication |
| Notifications | Email on job failure only | One message, after all retries |
| Maximum concurrent runs | 1, with queueing on | A second run waits for the first one instead of overlapping |
| Performance optimized | Off (standard mode) | Lower cost; the slower start (4 to 6 minutes according to the documentation) is acceptable for a daily run |

### 7.3 Behaviour and measurements

* If the ingestion fails, the notebook writes the error to the ingestion log before failing, the pipeline task does not start and the bronze table does not change. The next run recovers the missing period thanks to the 48-hour window.
* Measured on 2026-11-15 in performance optimized mode: a run takes between 1.3 and 1.7 minutes. The Spark work (writing the log and refreshing the bronze table) takes less than 20 seconds; most of the rest is compute start and orchestration.
* Measured in standard mode, on the scheduled runs of 2026-11-16 and 2026-11-17: a run takes between 8.4 and 10.4 minutes. The API call takes about 4 seconds; most of the rest is compute start for the two tasks.
* Measured on 2026-11-22, in standard mode: the scheduled run of 18:10 (two tasks) took 8 minutes 36 seconds. A manual run at 21:28 with the third task took 15 minutes 56 seconds: `ingest_update` 3 minutes 16 seconds, `refresh_pipeline` 8 minutes 34 seconds, `daily_checks` 4 minutes 5 seconds. The duration of `refresh_pipeline` varied from 5 minutes 3 seconds to 8 minutes 34 seconds between the two runs; the cause is not known.
* When a second run is queued, the waiting time is included in the duration of its first task.
* Known limitation: the job runs the code of the branch currently checked out in the Git folder. While the schedule is active, the Git folder must stay on a tested branch.

## 8. Metric views

### 8.1 What a metric view is

A metric view is a Unity Catalog object defined in YAML. It stores no data: it reads its source at query time. It declares **dimensions** (the columns to group by) and **measures** (the formulas). The `GROUP BY` is not written in the definition: whoever queries the view chooses it. Measures are read with `MEASURE()`, and `SELECT *` is not allowed. In Oracle terms it is close to an analytic view.

The definitions use `version: 1.1`, supported by the Free Edition (checked on 2026-11-06). Each definition has a `source` (the `FROM`), `dimensions`, `measures` and a `comment`. A measure can use other measures with `MEASURE()`.

### 8.2 The two metric views

Both are in `portfolio.uk_carbon_gold` and are defined in `setup/03_create_metric_views`.

| Metric view | Source | Dimensions | Measures |
|---|---|---|---|
| `metrics_carbon_intensity` | `mv_intensity_half_hour` | `region`, `date_uk`, `hour_uk`, `half_hour_utc`, `half_hour_uk`, `day_of_week`, `day_name`, `intensity_index` | `half_hours`, `total_intensity`, `avg_intensity`, `low_emission_half_hours`, `low_emission_share_pct` |
| `metrics_generation_mix` | `mv_fuel_mix_half_hour` | `region`, `date_uk`, `hour_uk`, `day_of_week`, `day_name`, `fuel_group`, `fuel` | `region_half_hours`, `total_percentage`, `avg_share_pct` |

| Measure | Formula |
|---|---|
| `half_hours` | `COUNT(1)` |
| `total_intensity` | `SUM(forecast_gco2_kwh)` |
| `avg_intensity` | `MEASURE(total_intensity) / MEASURE(half_hours)` |
| `low_emission_half_hours` | `COUNT(1)` of the rows with `intensity_index` in `very low`, `low` |
| `low_emission_share_pct` | `MEASURE(low_emission_half_hours) / MEASURE(half_hours) * 100` |
| `region_half_hours` | `COUNT(DISTINCT region_id, period_start_utc)` |
| `total_percentage` | `SUM(percentage)` |
| `avg_share_pct` | `MEASURE(total_percentage) / MEASURE(region_half_hours)` |

### 8.3 Design decisions

| Decision | Reason |
|---|---|
| Created by a setup notebook, not by the pipeline | A metric view contains no data, so there is nothing to refresh: it is created once, like a `CREATE VIEW` in a deployment script. The pipeline also refuses the statement: `CREATE VIEW ... WITH METRICS` inside the pipeline failed on 2026-11-20 with SQL state `0A000`. Every statement is `CREATE OR REPLACE`, so the notebook can be run again safely. Run it after the pipeline has created the gold layer |
| Two metric views, one per grain | Same reason as the two gold views (see 5.3): joining intensity and mix would repeat each forecast for the nine fuels |
| Averages are a sum divided by a count, computed at query time | An average of averages is wrong when the groups have different sizes. Illustrative example: a day with 48 half hours and a sum of 4,800 (average 100) and a day with 34 half hours and a sum of 6,800 (average 200). The average of the two averages is 150; the correct average is 11,600 / 82 = 141.46 |
| The share of a fuel group is the sum of its percentages divided by the number of region and half hour pairs | In the generation mix each region and half hour is one pie, and each row is one slice. Example: London, 2026-11-19 12:00 UTC, Renewable = 18.6%. `AVG(percentage)` over its four rows would give 4.65, the average slice. The denominator is `COUNT(DISTINCT region_id, period_start_utc)`, counted from the data, not a number written by hand |
| Names in snake_case, readable labels in `display_name` | The names are easy to use in SQL. The label replaces the alias in the column headers of a query (checked in SQL queries on 2026-11-21) |
| Comments in the `comment:` field | A YAML comment with `#` was removed when the notebook was saved with `version: 1.1` |
| Low emission = `very low` plus `low` | The labels of the API index are used as given. No official source for the limits of the classes has been found yet |
| Averages across regions are simple | They are not weighted by regional demand, which the API does not provide (see 1.4). It is a known limit |
| The half hour dimensions use the UTC start as the key, and London time only as a label | In October the London clock repeats 01:00 and 01:30: with London time as the key, two different half hours would be merged. The data have 36,202 UTC half hours and 36,198 London times (see P8 in 8.4) |

### 8.4 Reconciliation

Each metric view is compared with a query written by hand, with formulas that do not use the metric view. The two results are joined with a `FULL OUTER JOIN` on the grouping keys. The result has one row of counters: cells compared, cells present on one side only (`missing_on_one_side`), and the number of cells where the pie count, the total or the average differ. Tolerances: 1e-9 for averages, 1e-6 for totals. One test per grouping, so that every dimension is used at least once. `missing_on_one_side` is the only guard on the keys: a NULL in a comparison is never counted as a difference. For the generation mix the query written by hand has two levels (the sum for each pie, then the average over the pies), because a direct `AVG(percentage)` is the average slice. The test queries run in a notebook outside the repository.

| Test | Metric view and grouping | Cells | Result |
|---|---|---|---|
| P1 | `metrics_carbon_intensity`, by region | 14 | No difference (2026-11-20) |
| P2 | `metrics_carbon_intensity`, by hour and day of week | 168 (24 x 7) | No difference; the 7 pairs of day number and day name are consistent (2026-11-20) |
| P3 | `metrics_carbon_intensity`, by date and intensity index | 3,735 | No difference. 751 days x 5 classes would be 3,755: 20 combinations do not occur in the data, and the metric view and the query written by hand agree on them (2026-11-20) |
| P4 | `metrics_generation_mix`, by fuel group | 3 | No difference (2026-11-21) |
| P5 | `metrics_generation_mix`, by region and fuel group | 42 (14 x 3) | No difference (2026-11-21) |
| P6 | `metrics_generation_mix`, by date and fuel | 6,768 (752 x 9) | No difference. A direct `AVG(percentage)` is valid here, because with one fuel each pie has one row (2026-11-21) |
| P7 | `metrics_generation_mix`, by hour, day of week and fuel group (the heatmap) | 504 (24 x 7 x 3) | No difference; 7 pairs of day number and day name (2026-11-21) |
| P8 | `metrics_carbon_intensity`, by half hour (`half_hour_utc`, `half_hour_uk`) | 36,202 | No difference; every half hour has the 14 regions; 36,198 distinct London times, 4 fewer than the UTC half hours because of the two October clock changes in the data (2026-11-25) |

On 2026-11-21 the check cell of `metrics_generation_mix` in the setup notebook gave 504,140 region half hours, 504,140 rows in `mv_intensity_half_hour`, and 99.9968 for the shares of all fuels (they add up to about 100).

### 8.5 Not checked yet

* The `display_name` labels in Genie. In a dashboard they are used in the list of fields (checked on 2026-11-25).

## 9. Daily checks

### 9.1 Approach

The notebook `checks/01_daily_job_checks` runs one SQL query after the Job `uk_carbon_refresh`. It returns one row per check, `OK` or `KO`, and a summary row (id 99) with the number of checks in `KO`.

| Decision | Reason |
|---|---|
| Every check is a relation that holds every day, not a number written by hand | For example "gold rows = silver rows". The notebook needs no edit when new data arrives |
| A NULL result is `KO` | The summary counts `passed IS NOT TRUE`, so a comparison with NULL is not a silent `OK` |
| Observations are left out | Facts such as the known source gaps (see 1.3) are not relations that must hold every day |
| The first run was validated by hand | The checks were tested on data broken on purpose (6 `KO` as expected) and validated on the real run of 2026-11-20 against a manual check of the same day |
| A new check is also tried in `KO` | Check 26 was tried with `+ 1` in its condition: row 26 in `KO`, summary 1. With the condition restored: 27 rows, 26 `OK`, summary 0 |

### 9.2 Checks by layer

| Layer | Checks | What is checked |
|---|---|---|
| Ingestion | 1 to 3 | Last call has HTTP status 200; last call is complete; the window of the last call ends less than 26 hours ago, so the notebook fails if the Job did not run |
| Bronze | 4 to 7 | Bronze rows equal the files in the log; no rescued data; every file has the half hours of the log (`json_array_length(data)` against `n_half_hours`); files compared equal bronze rows |
| Silver | 8 to 16 | One row per key in `intensity` and `generation_mix`; 14 regions; no NULL forecast or percentage; last half hour equal to the end of the log window minus 30 minutes; 9 fuels; `generation_mix` rows equal 9 x `intensity` rows; `regions` has 14 rows from one file |
| Gold | 17 to 24 | Gold rows equal silver rows, one per key, for both views; no NULL names or groups; London time is `timestamp_ntz`; same last half hour as silver; group rows equal region half hours x 4 / 2 / 3; the fuels add up to 100 +/- 0.5 |
| Metric views | 25 and 26 | `half_hours` of `metrics_carbon_intensity` equals the gold rows; `region_half_hours` of `metrics_generation_mix` equals the gold intensity rows |
| Summary | 99 | Number of checks in `KO` |

On the runs of 2026-11-21 and 2026-11-22 the checks returned `OK` and the summary 0 (on 2026-11-21 there were 25 checks; check 26 was added the same evening and was `OK`).

### 9.3 Failing the Job

The notebook stores the query in the temporary view `daily_checks` and ends with a cell that calls `assert_true`. The cell returns NULL when every check is `OK`. It raises `USER_RAISED_EXCEPTION` (SQLSTATE `P0001`) with a message when at least one check is `KO`, or when the view is empty. In a Job task this makes the task fail. `assert_true` also fails on a NULL condition (checked on 2026-11-22), in line with the rule that a NULL is `KO`.

Tested on 2026-11-22 in a test Job with one task that runs the notebook: serverless compute, failure email, no retries (a check in `KO` does not recover by trying again).

| Case | Result |
|---|---|
| All checks `OK` | Run succeeded in 32 seconds |
| Check 25 with `+ 1` in its condition | Run failed in 31 seconds: the first two cells passed, the `assert_true` cell failed with the message of the notebook, and the failure email arrived the same minute |

The email is generic ("Task checks failed ... see run output for details"): the check in `KO` is read in the run output. The test also showed that the metric views read by checks 25 and 26 can be queried from a Job task.

### 9.4 Third task of the Job

The checks are the third task of `uk_carbon_refresh`, `daily_checks`, configured on 2026-11-22. It runs the notebook `checks/01_daily_job_checks` from the Git folder, after `refresh_pipeline`, on serverless compute. A `KO` turns the run red and sends the failure email, as tested in 9.3.

First run, started by hand on 2026-11-22 at 21:28 in Italy (20:28 in London); the task `daily_checks` started at 21:40 and succeeded in 4 minutes 5 seconds, 27 rows, all `OK`, summary row 0. The two queries take about 27 and 20 seconds, so most of the duration is outside the queries. The likely cause is the start of the serverless compute, but it was not measured.

First automatic run, started by the scheduler on 2026-11-23 at 18:10 in Italy (17:10 in London): the three tasks succeeded in 14 minutes 50 seconds (`ingest_update` 3 minutes 47 seconds, `refresh_pipeline` 7 minutes 24 seconds, `daily_checks` 3 minutes 39 seconds), 27 rows, all `OK`, summary row 0.

Proven on 2026-11-24 inside the real job: a manual run at 22:30 in Italy (21:30 in London), from a test branch with `+ 1` in the condition of check 25, failed as expected: row 25 `KO`, summary row 1, failure email at 22:44. The test branch was then deleted.
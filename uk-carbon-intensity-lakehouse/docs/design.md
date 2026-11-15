# UK Carbon Intensity Lakehouse: design document

Status: draft, sections 1 to 5. Last updated: 2026-11-15.

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
| Sum of the generation mix | not stated | Between 99.8 and 100.1 on the day checked (rounding) |
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

### 1.5 Design consequences

| Measured fact | Design decision |
|---|---|
| Endpoint without `regionid` returns all regions in one call | One call per time block instead of 14 |
| 14-day limit on that endpoint | Backfill in **7-day blocks**, from 00:00 UTC to 00:00 UTC: half the limit as safety margin, and readable file names. 106 calls for the 24-month backfill |
| One extra half hour at the start of every response | The half hour at each block boundary arrives twice. Deduplication on the key (region, half-hour start) in the silver layer |
| Ids 15 to 18 are aggregates | Silver keeps only ids 1 to 14 (blocking data quality rule) |
| HTTP 200 with zero rows is possible | The ingestion log records the number of half hours received for every call |
| A call can return fewer half hours than expected (year end, source gaps) | The ingestion log stores the expected half hours and a completeness flag for every call; incomplete calls are flagged, not blocked |
| Rate limit not published | Pause between calls; retries with exponential backoff on 429 and 5xx; HTTP 400 stops the task without retries |
| API may change without notice | The raw response is stored unchanged, so the silver layer can be rebuilt without calling the API again |
| Multi-day ranges lose data after 23:00 UTC on 31 December | The days around each year end are downloaded again with one-day ranges (14 extra files in the backfill). The 7-day files are kept as a record; the overlap is removed by the silver deduplication |
| Two gaps in the source (25 half hours per region) | Documented as a known limitation. The completeness rule (48 half hours per region and UTC day) is a warning, not a blocking rule |

## 2. Architecture

### 2.1 Layers and schemas

The project uses one Unity Catalog schema per layer, all in the `portfolio` catalog. The schemas are created by `setup/00_create_catalog_schema_volume` and `setup/02_create_layer_schemas`.

| Schema | Layer | Content |
|---|---|---|
| `portfolio.uk_carbon` | Ingestion | Landing volume (raw API responses as JSON files) and ingestion log |
| `portfolio.uk_carbon_bronze` | Bronze | Raw API responses read by Auto Loader, all fields as strings |
| `portfolio.uk_carbon_silver` | Silver | Typed, filtered and deduplicated tables |
| `portfolio.uk_carbon_gold` | Gold | Materialized views and metric view for the dashboard |

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

## 4. Pipeline `uk_carbon_pipeline`

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

Source files are plain Python or SQL files, not notebooks. They are numbered (`01_bronze.py`, then silver and gold) for readers; the execution order is decided by the pipeline graph.

## 5. Orchestration: job `uk_carbon_refresh`

### 5.1 Tasks

| Task | Type | Settings | Retries |
|---|---|---|---|
| `ingest_update` | Notebook `ingestion/01_ingest_regional_api` from the Git folder, serverless | Parameters `mode` = `update`, `lookback_hours` = `48` | 2 retries (3 attempts in total), 10 minutes apart; serverless auto-optimization disabled |
| `refresh_pipeline` | Pipeline `uk_carbon_pipeline`, no full refresh | Depends on `ingest_update`; runs only if it succeeded | 1 retry (2 attempts in total), 5 minutes apart |

### 5.2 Job settings

| Setting | Value | Reason |
|---|---|---|
| Schedule | Every day at 18:10, Europe/Rome | One file per day is enough for this project |
| Update window | The last 48 hours, ending at the current time rounded down to the half hour (2 x 48 + 1 = 97 half hours, see 1.3) | The window is twice the interval: if one day fails, the next run fills the gap. The overlap is removed by the silver deduplication |
| Notifications | Email on job failure only | One message, after all retries |
| Maximum concurrent runs | 1, with queueing on | A second run waits for the first one instead of overlapping |
| Performance optimized | Off (standard mode) | Lower cost; the slower start (4 to 6 minutes according to the documentation) is acceptable for a daily run |

### 5.3 Behaviour and measurements

* If the ingestion fails, the notebook writes the error to the ingestion log before failing, the pipeline task does not start and the bronze table does not change. The next run recovers the missing period thanks to the 48-hour window.
* Measured on 2026-11-15 in performance optimized mode: a run takes between 1.3 and 1.7 minutes. The Spark work (writing the log and refreshing the bronze table) takes less than 20 seconds; most of the rest is compute start and orchestration. Measurements in standard mode will follow.
* When a second run is queued, the waiting time is included in the duration of its first task.
* Known limitation: the job runs the code of the branch currently checked out in the Git folder. While the schedule is active, the Git folder must stay on a tested branch.
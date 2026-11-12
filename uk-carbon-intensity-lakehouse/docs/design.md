# UK Carbon Intensity Lakehouse: design document

Status: draft, section 1 of N. Last updated: 2026-11-12.

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

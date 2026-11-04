# Source Analysis: UK Carbon Intensity API

**Status:** completed on 24 October 2026
**Scope:** selection and inspection of the data source for the first project of this repository
**Evidence:** every measured behaviour below comes from the notebook [`API_data_inspection.ipynb`](API_data_inspection.ipynb), run on Databricks Free Edition

> **Attribution.** Data: Carbon Intensity API, National Energy System Operator (NESO), licensed under CC BY 4.0 and used under the [API Terms of Use](https://github.com/carbon-intensity/terms). This project is not affiliated with or endorsed by NESO.

---

## 1. Why this document

The project will build a lakehouse on Databricks: a Web API feeds the **bronze** layer, the **silver** layer holds cleaned and deduplicated data, and the **gold** layer serves a dashboard and a Databricks App.

Before designing any layer, I wanted to know how the source really behaves. API documentation describes the intended contract; it rarely describes limits, edge cases and data quality. This document records what was measured, where it differs from the documentation, and what it implies for the design.

## 2. Selection criteria

1. **Open data** with a clear licence for reuse.
2. **Homogeneous units of comparison**: the same territories over time, so that comparisons are meaningful.
3. A domain close to my background: **public utilities** (electricity and gas).
4. Reachable from **Databricks Free Edition**, where outbound traffic is limited to a set of trusted domains whose list is not published.

## 3. Candidate sources

| Source | Licence and terms | Outcome |
|---|---|---|
| **UK Carbon Intensity API (NESO)** | CC BY 4.0 stated in the documentation, plus API Terms of Use | **Selected.** Regional data at 30 minute granularity, reachable from Free Edition |
| Open-Meteo (weather) | Terms still under review | Reachable. Kept for a later phase (weather versus generation mix) |
| Energy-Charts, Fraunhofer ISE | Non commercial and journalistic use with attribution, as published by the provider; official legal text to be confirmed | Reachable. National data only. Possible future extension to Italy and Europe |
| Terna Download Center (Italian load) | **No licence for data reuse found**; the available Terms of Use cover only the developer portal | **Discarded** |

Two side observations from the candidates, both useful as reminders that every source has quirks:

* The Terna CSV uses the comma both as field separator and as decimal separator, which makes parsing unreliable.
* Energy-Charts returns 15 minute values for most countries but hourly values for Switzerland: granularity is not uniform across countries.

## 4. Licence and Terms of Use

Both the [official documentation](https://carbon-intensity.github.io/api-definitions/#carbon-intensity-api-v2-0-0) and the [Terms of Use repository](https://github.com/carbon-intensity/terms) were read.

* **Licence.** The documentation labels the API with CC BY 4.0. In the Terms of Use repository, CC BY 4.0 is linked as the licence of the terms document itself, while use of the API is granted under the Terms of Use. This project cites both.
* **Attribution.** Neither text gives an official wording. The attribution used in this repository, the dashboard and the App is the one shown at the top of this document, as **plain text without any logo**.
* **Rate limit.** The Terms of Use state that NESO applies a rate limit and may block applications making a large number of calls. **The value is not published.**
* **Restrictions relevant to this project:** no use of the NESO name, word mark or logo without written approval; no suggestion of endorsement; no application that replaces the core experience of NESO services; no resale or sublicensing of access.
* **Risks:** the API is provided "as is", may contain errors or interruptions, and may change or be discontinued without notice; the Terms of Use themselves may change without notice.

This is my reading of the published texts, not legal advice. For a non commercial portfolio project I found no obstacle to use.

## 5. Measured behaviour

All tests use region 13 (London) unless stated otherwise, with a one second pause between calls.

### 5.1 Maximum date range per call

The documentation states a 14 day limit for the **national** endpoint `/intensity/{from}/{to}` and no limit for the regional endpoints.

| Requested range | HTTP | Rows | Requested half hours |
|---|---|---|---|
| 1 day | 200 | 49 | 48 |
| 7 days | 200 | 337 | 336 |
| 14 days | 200 | 673 | 672 |
| 15 days | 200 | 721 | 720 |
| 31 days | 200 | 1,489 | 1,488 |
| 59 days | 200 | 2,833 | 2,832 |
| 60 days minus 30 minutes | 200 | 2,880 | 2,879 |
| 60 days | 400 | none | 2,880 |
| 92, 183, 366 days | 400 | none | |

The 400 response says that the date range is greater than 60 days.

**Conclusion:** the regional limit is **60 days of effective range**, and the check is applied to the window **after** the API has extended it by 30 minutes at the start (see 5.2). With 60 days minus 30 minutes the API returns exactly 2,880 rows, that is 60 full days.

### 5.2 One extra half hour at the start

Every response contains **one row more** than requested. Asking for one day from 00:00 returns a first row that starts at 23:30 of the previous day: the API also returns the half hour that **ends** at the requested start.

**Implication:** when the history is loaded in consecutive blocks (for example September, then October), the half hour at the boundary arrives **twice**. Deduplication in silver is therefore not a theoretical precaution but a measured necessity.

### 5.3 Depth of the regional history

| Requested day | HTTP | Rows | First row starts |
|---|---|---|---|
| 2017-09-01 | 200 | 0 | |
| 2018-05-01 | 200 | 0 | |
| 2018-06-01 | 200 | 49 | 2018-05-31T23:30Z |
| 2019-01-01 | 200 | 49 | 2018-12-31T23:30Z |
| 2020-01-01 | 200 | 49 | 2019-12-31T23:30Z |
| 2022-01-01 | 200 | 49 | 2021-12-31T23:30Z |
| 2024-01-01 | 200 | 49 | 2023-12-31T23:30Z |
| 2026-01-01 | 200 | 49 | 2025-12-31T23:30Z |

* Regional history **starts between 1 May and 1 June 2018**: more than eight years of data.
* **Missing data is not an error**: the API answers HTTP 200 with zero rows.
* Only one day every one or two years was sampled; possible gaps in between are not yet known.

### 5.4 All regions in a single call

Without the `regionid` segment, `/regional/intensity/{from}/{to}` returns **every region** for the requested range. For one day the response holds 49 half hours, each containing 18 regions, each containing 9 fuels.

```
data (list of half hours)
 └─ from, to, regions (list of 18 regions)
              └─ regionid, dnoregion, shortname,
                 intensity {forecast, index},
                 generationmix (list of fuels)
                               └─ fuel, perc
```

Notes on the structure:

* The nesting order is **half hour → region → fuel**. The endpoint with `regionid` uses the opposite order (region → half hour).
* `dnoregion` holds the name of the distribution network operator, useful for a region dimension.
* The key order inside `generationmix` changes between elements: fields must be read by name, never by position.

**Call budget for the historical load**, using monthly blocks:

| Approach | Calls per month | Calls for 3 years |
|---|---|---|
| One call per region (`regionid`) | 14 | 504 |
| All regions in one call | 1 | **36** |

### 5.5 Data quality on one day (1 September 2026)

| Check | Result |
|---|---|
| Regions per half hour | 18 in all 49 half hours |
| Region ids | 1 to 18, no gaps |
| Fuels | 9 (biomass, coal, imports, gas, nuclear, other, hydro, solar, wind), 882 occurrences each, that is 49 × 18: every region reports every fuel in every half hour, zeros included |
| Generation mix total per (half hour, region) | between 99.8 and 100.1; 0 of 882 totals more than 1 point away from 100, a deviation explained by the rounding of single percentages |
| Null intensity forecasts | 0 |

These results cover **one day only**: they guide the design of the rules, they do not fix their thresholds.

### 5.6 Regions 15 to 18 are aggregates

Ids 15 to 18 are England, Scotland, Wales and GB, which contain territories already covered by regions 1 to 14. Mixing them with the regions is like mixing detail rows and subtotal rows of a SQL `GROUP BY ROLLUP`: any sum or comparison across all 18 rows counts the same territory twice. Totals computed **within** one region (such as the generation mix total) are not affected.

Regions are electricity distribution areas, not administrative boundaries, and the documentation flags regional data as beta.

### 5.7 Documented versus measured

| Aspect | Documentation | Measured |
|---|---|---|
| Maximum range per call (regional) | 14 days, stated for the national endpoint | 60 days of effective range |
| Rows returned | not stated | one extra half hour at the start |
| Missing data | not stated | HTTP 200 with zero rows |
| Start of regional history | not stated | between 1 May and 1 June 2018 |
| Error format | not stated | JSON containing `\r` characters, HTTP 400 for ranges too long |
| Rate limit | not stated in the documentation | stated in the Terms of Use, value not published |

## 6. Implications for the design

These are inputs for the analysis and design document, not final decisions.

**Ingestion**

* Use the endpoint **without** `regionid`: one call per monthly block.
* Monthly blocks stay well below the 60 day limit and are easy to reload ("reload October 2024").
* One second pause between calls; retries with increasing wait on HTTP 429 and 5xx.
* Record "HTTP 200 with zero rows" explicitly in the load metadata.
* Initial history depth to be decided; two or three years (24 or 36 calls) are enough for the dashboard.

**Bronze**

* One row per API call: the raw JSON as received, plus load metadata (requested month, load timestamp, request parameters, HTTP status, number of half hours received, batch id).
* No transformation: silver can be rebuilt from bronze without calling the API again.

**Silver: two tables with different grain**

* **Carbon intensity:** one row per (region, half hour start).
* **Generation mix:** one row per (region, half hour start, fuel).
* Both are obtained by flattening the nested structure twice. A small region dimension (id, short name, distribution network operator) comes for free.

**Proposed data quality rules**

| Rule | Type | Evidence |
|---|---|---|
| Keep only region ids 1 to 14 | blocking | 5.6 |
| Unique key (region, half hour start), duplicates removed | blocking, with deduplication | 5.2 |
| Intensity forecast not null | to be decided | 5.5 |
| Exactly 9 fuel rows per (region, half hour) | warning | 5.5 |
| Generation mix total between 99 and 101 | warning | 5.5 |
| 48 half hours per region per UTC day | warning (completeness) | 5.3 |

**Blocking** rules reject the row; **warning** rules keep it and flag it, so that unexpected data is seen and assessed instead of silently lost.

## 7. Limitations of this analysis

* Quality checks cover a single day; thresholds must be confirmed on the full history.
* History depth was sampled, not scanned day by day.
* The 60 day limit was measured on the endpoint with `regionid`; with monthly blocks the same limit on the endpoint without `regionid` does not matter, but it has not been measured.
* All results describe the API as it behaved on 24 October 2026. The provider may change it without notice, which is one more reason to keep the raw responses in bronze.

## 8. Sources

* Carbon Intensity API, official documentation: <https://carbon-intensity.github.io/api-definitions/#carbon-intensity-api-v2-0-0>
* Carbon Intensity API, Terms of Use: <https://github.com/carbon-intensity/terms>
* Notebook with all tests and outputs: [`API_data_inspection.ipynb`](API_data_inspection.ipynb)

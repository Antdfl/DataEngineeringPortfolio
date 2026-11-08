# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 01 Ingestion: UK Carbon Intensity regional API to a Unity Catalog Volume
# MAGIC
# MAGIC **Area 1 of the architecture.** This notebook only calls the API and lands the raw response.
# MAGIC It does not transform, filter or deduplicate anything (that is the job of the SDP pipeline).
# MAGIC
# MAGIC **Contract with the pipeline**
# MAGIC * Folder: `/Volumes/portfolio/uk_carbon/landing/`
# MAGIC * One file per API call, named `regional_<from>_<to>_<download timestamp>.json`
# MAGIC * Content: the API response, byte for byte
# MAGIC
# MAGIC **Every call is logged** in `portfolio.uk_carbon.ingestion_log` (also the calls that return HTTP 200 with zero rows).
# MAGIC
# MAGIC **Modes**
# MAGIC * `backfill`: one call per block of `block_days` days between `start_date` (included) and `end_date` (excluded).
# MAGIC   Blocks already downloaded successfully are skipped when `skip_done` is `true`
# MAGIC   (keep `start_date` and `block_days` unchanged between runs, otherwise the blocks do not match).
# MAGIC * `update`: one call for the last `lookback_hours` hours, rounded down to the half hour.
# MAGIC
# MAGIC **Measured limit (7 November 2026):** the endpoint without `regionid` accepts windows up to
# MAGIC 14 days minus 30 minutes and rejects 14 days or more with HTTP 400 ("greater than 14 days").
# MAGIC Backfill blocks are 7 days, from 00:00 UTC to 00:00 UTC (half the limit as safety margin).
# MAGIC The notebook caps blocks and lookbacks at 13 whole days.
# MAGIC Each response also contains the half hour ending at the requested start, so the half hour at every block
# MAGIC boundary arrives twice; the silver layer deduplicates it.
# MAGIC
# MAGIC **Failure policy:** 429, 5xx and network errors are retried with growing waits; any other HTTP error (for example 400) is not retried.
# MAGIC After a failed call the notebook stops with an error. Re-running it in `backfill` mode resumes from the first missing block.
# MAGIC
# MAGIC Data: Carbon Intensity API, National Energy System Operator (NESO), CC BY 4.0.

# COMMAND ----------

import json
import os
import time
import uuid
from datetime import datetime, timedelta, timezone

import requests
from pyspark.sql import types as T

CATALOG = "portfolio"
SCHEMA = "uk_carbon"
VOLUME = "landing"

LANDING_DIR = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
LOG_TABLE = f"{CATALOG}.{SCHEMA}.ingestion_log"

BASE_URL = "https://api.carbonintensity.org.uk"
PAUSE_SECONDS = 1        # pause after every call: the terms of use mention a rate limit (value not published)
MAX_ATTEMPTS = 5         # attempts per call for 429, 5xx and network errors
MAX_WINDOW_DAYS = 13     # measured: 14 days minus 30 min is accepted, 14 days is rejected; whole days stay below it
MAX_LOOKBACK_HOURS = MAX_WINDOW_DAYS * 24

# COMMAND ----------

# Pure helper functions (no Spark, no dbutils): easy to test on their own.

def parse_day(text):
    """'YYYY-MM-DD' -> timezone aware datetime at 00:00 UTC."""
    return datetime.strptime(text.strip(), "%Y-%m-%d").replace(tzinfo=timezone.utc)


def day_blocks(start, end, block_days):
    """Consecutive windows [from, to) of `block_days` days covering [start, end); the last one may be shorter."""
    if not 1 <= block_days <= MAX_WINDOW_DAYS:
        raise ValueError(f"block_days must be between 1 and {MAX_WINDOW_DAYS}")
    current = start
    while current < end:
        following = min(current + timedelta(days=block_days), end)
        yield current, following
        current = following


def update_window(now, lookback_hours):
    """Window [end - lookback, end) where `end` is `now` rounded down to the half hour."""
    end = now.replace(minute=(now.minute // 30) * 30, second=0, microsecond=0)
    return end - timedelta(hours=lookback_hours), end


def iso_z(moment):
    """Format used by the API in the URL and in the log: 2026-09-01T00:00Z."""
    return f"{moment:%Y-%m-%dT%H:%MZ}"


def build_url(start, end):
    # No regionid: all regions in one call.
    return f"{BASE_URL}/regional/intensity/{iso_z(start)}/{iso_z(end)}"


def landing_file_name(start, end, downloaded_at):
    return f"regional_{start:%Y-%m-%d}_{end:%Y-%m-%d}_{downloaded_at:%Y%m%dT%H%MZ}.json"


def fetch(start, end):
    """One logical call with retries. Returns a dict: url, status, body (bytes or None), attempts, error."""
    url = build_url(start, end)
    status, error, attempts = None, None, 0
    while attempts < MAX_ATTEMPTS:
        attempts += 1
        response = None
        try:
            response = requests.get(url, headers={"Accept": "application/json"}, timeout=120)
            status = response.status_code
        except requests.exceptions.RequestException as exc:
            status, error = None, repr(exc)

        if status == 200:
            return {"url": url, "status": 200, "body": response.content, "attempts": attempts, "error": None}

        if response is not None:
            error = repr(response.text[:300])   # repr(): the API error text contains \r characters
            if status != 429 and status < 500:
                # 400 and other client errors: retrying cannot help
                return {"url": url, "status": status, "body": None, "attempts": attempts, "error": error}

        wait = 2 ** attempts                    # 2, 4, 8, 16, 32 seconds
        retry_after = response.headers.get("Retry-After") if response is not None else None
        if retry_after and retry_after.isdigit():
            wait = max(wait, int(retry_after))
        time.sleep(wait)

    return {"url": url, "status": status, "body": None, "attempts": attempts, "error": error}


def count_half_hours(body):
    """Number of items in `data` (one per half hour). Raises ValueError if the body is not the expected JSON."""
    try:
        payload = json.loads(body)
        return len(payload["data"])
    except (ValueError, KeyError, TypeError) as exc:
        raise ValueError(f"Unexpected response body: {exc!r}")

# COMMAND ----------

# Widgets (they also work as Job parameters)
dbutils.widgets.dropdown("mode", "update", ["update", "backfill"], "Mode")
dbutils.widgets.text("start_date", "2024-11-01", "Backfill start (YYYY-MM-DD)")
dbutils.widgets.text("end_date", "2026-11-01", "Backfill end, excluded (YYYY-MM-DD)")
dbutils.widgets.text("block_days", "7", "Backfill: days per call (max 13)")
dbutils.widgets.text("lookback_hours", "6", "Update: hours to look back")
dbutils.widgets.dropdown("skip_done", "true", ["true", "false"], "Backfill: skip blocks already downloaded")

MODE = dbutils.widgets.get("mode")
SKIP_DONE = dbutils.widgets.get("skip_done") == "true"

# COMMAND ----------

# Log table (created once) and helpers to read and write it.

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {LOG_TABLE} (
  ingestion_id        STRING    COMMENT 'Unique id of the logged call',
  run_ts              TIMESTAMP COMMENT 'When the call finished (UTC)',
  mode                STRING    COMMENT 'backfill or update',
  requested_from_utc  STRING    COMMENT 'Requested window start, e.g. 2026-09-01T00:00Z',
  requested_to_utc    STRING    COMMENT 'Requested window end',
  url                 STRING    COMMENT 'Full URL called',
  http_status         INT       COMMENT 'Last HTTP status, NULL if no response was received',
  n_half_hours        INT       COMMENT 'Items in data (0 = HTTP 200 with no rows)',
  file_name           STRING    COMMENT 'File written in the landing Volume, NULL if none',
  attempts            INT       COMMENT 'Attempts used for this call',
  elapsed_seconds     DOUBLE    COMMENT 'Total time including retries',
  error               STRING    COMMENT 'Error text when the call failed'
)
COMMENT 'One row per API call made by the ingestion notebook'
""")

LOG_SCHEMA = T.StructType([
    T.StructField("ingestion_id", T.StringType()),
    T.StructField("run_ts", T.TimestampType()),
    T.StructField("mode", T.StringType()),
    T.StructField("requested_from_utc", T.StringType()),
    T.StructField("requested_to_utc", T.StringType()),
    T.StructField("url", T.StringType()),
    T.StructField("http_status", T.IntegerType()),
    T.StructField("n_half_hours", T.IntegerType()),
    T.StructField("file_name", T.StringType()),
    T.StructField("attempts", T.IntegerType()),
    T.StructField("elapsed_seconds", T.DoubleType()),
    T.StructField("error", T.StringType()),
])


def write_log(row):
    spark.createDataFrame([row], schema=LOG_SCHEMA).write.mode("append").saveAsTable(LOG_TABLE)


def already_downloaded():
    """Set of (from, to) strings downloaded with HTTP 200, at least one half hour and a file."""
    rows = (spark.table(LOG_TABLE)
            .where("http_status = 200 AND n_half_hours > 0 AND file_name IS NOT NULL")
            .select("requested_from_utc", "requested_to_utc")
            .distinct()
            .collect())
    return {(r.requested_from_utc, r.requested_to_utc) for r in rows}

# COMMAND ----------

# One window: call, write the file, log. Stops the notebook on any failure.

def ingest_window(start, end):
    started = time.time()
    result = fetch(start, end)
    downloaded_at = datetime.now(timezone.utc)

    n_half_hours, file_name, error = None, None, result["error"]
    if result["status"] == 200:
        try:
            n_half_hours = count_half_hours(result["body"])
        except ValueError as exc:
            error = str(exc)
        else:
            file_name = landing_file_name(start, end, downloaded_at)
            with open(f"{LANDING_DIR}/{file_name}", "wb") as f:
                f.write(result["body"])           # the response as received, no re-serialisation

    ok = result["status"] == 200 and error is None
    write_log((
        str(uuid.uuid4()), downloaded_at, MODE, iso_z(start), iso_z(end), result["url"],
        result["status"], n_half_hours, file_name, result["attempts"],
        round(time.time() - started, 2), None if ok else error,
    ))
    time.sleep(PAUSE_SECONDS)

    label = f"{iso_z(start)} -> {iso_z(end)}"
    if not ok:
        raise RuntimeError(f"Ingestion stopped. Window {label}: HTTP {result['status']}, error {error}. "
                           f"See {LOG_TABLE}.")
    note = "  (HTTP 200 with zero rows, nothing to load)" if n_half_hours == 0 else ""
    print(f"OK  {label}: {n_half_hours} half hours, {result['attempts']} attempt(s), file {file_name}{note}")

# COMMAND ----------

# Run

if MODE == "backfill":
    start, end = parse_day(dbutils.widgets.get("start_date")), parse_day(dbutils.widgets.get("end_date"))
    if start >= end:
        raise ValueError("start_date must be before end_date")
    done = already_downloaded() if SKIP_DONE else set()
    windows = list(day_blocks(start, end, int(dbutils.widgets.get("block_days"))))
    print(f"Backfill: {len(windows)} windows, {len(done)} windows already logged as downloaded")
    for w_start, w_end in windows:
        if (iso_z(w_start), iso_z(w_end)) in done:
            print(f"SKIP {iso_z(w_start)} -> {iso_z(w_end)}: already downloaded")
            continue
        ingest_window(w_start, w_end)
else:
    lookback = int(dbutils.widgets.get("lookback_hours"))
    if not 1 <= lookback <= MAX_LOOKBACK_HOURS:
        raise ValueError(f"lookback_hours must be between 1 and {MAX_LOOKBACK_HOURS}")
    w_start, w_end = update_window(datetime.now(timezone.utc), lookback)
    ingest_window(w_start, w_end)

# COMMAND ----------

# Check: last calls in the log, then files in the Volume

display(spark.table(LOG_TABLE).orderBy("run_ts", ascending=False).limit(30))

# COMMAND ----------

print(len(os.listdir(LANDING_DIR)), "files in", LANDING_DIR)
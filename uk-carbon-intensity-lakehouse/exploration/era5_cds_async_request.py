# Databricks notebook source
# MAGIC %md
# MAGIC # ERA5 from the Copernicus CDS: asynchronous request and reading on Databricks Free Edition
# MAGIC
# MAGIC **Purpose:** check that the Climate Data Store (CDS) is reachable from Databricks Free Edition,
# MAGIC download one month of hourly ERA5 data for a test area around London, and run quality checks on it.
# MAGIC
# MAGIC **Prerequisites (no secret values in this file):**
# MAGIC - a CDS account with the licence of the `reanalysis-era5-single-levels` dataset accepted on the dataset page
# MAGIC - a personal CDS token stored in the secret `token` of the scope `cds` (created with the Databricks CLI)
# MAGIC
# MAGIC **Usage rules adopted** (service protection measures): only one active request at a time,
# MAGIC no automatic retries, no cancel and resubmit loops.
# MAGIC
# MAGIC **Flow:** 1) submit the request without waiting, 2) download once the request is complete
# MAGIC (hours later, even from a new session), 3) extract the ZIP, 4) quality checks.
# MAGIC
# MAGIC *Contains modified Copernicus Climate Change Service information. ERA5 data: Hersbach et al., ECMWF / Copernicus.
# MAGIC Do not publish the downloaded files until the dataset licence has been checked.*

# COMMAND ----------

# MAGIC %pip install --quiet "cdsapi>=0.7.4" xarray netCDF4

# COMMAND ----------

# Connection parameters. The token is read ONLY from Databricks secrets and is never printed.
CDS_URL = "https://cds.climate.copernicus.eu/api"
CDS_KEY = dbutils.secrets.get(scope="cds", key="token")

# COMMAND ----------

# Step 1: submit the request WITHOUT waiting for the result.
# wait_until_complete=False returns control immediately; delete=False keeps the result on the server.
# Run this cell only ONCE and note the request_id (do not publish it in the repo).
import cdsapi

client = cdsapi.Client(url=CDS_URL, key=CDS_KEY, wait_until_complete=False, delete=False)

request = {
    "product_type": ["reanalysis"],
    "variable": [
        "2m_temperature",
        "100m_u_component_of_wind",
        "100m_v_component_of_wind",
        "surface_solar_radiation_downwards",
        "total_cloud_cover",
    ],
    "year": ["2026"],
    "month": ["09"],
    "day": [f"{d:02d}" for d in range(1, 31)],
    "time": [f"{h:02d}:00" for h in range(24)],
    "area": [52.0, -1.0, 51.0, 0.0],   # north, west, south, east: test area around London
    "data_format": "netcdf",
    "download_format": "unarchived",
}

r = client.retrieve("reanalysis-era5-single-levels", request)
print("request_id:", getattr(r, "request_id", "attribute not found"))

# COMMAND ----------

# Step 2: check the status on the "Your requests" page of the CDS website.
# When the status is "Successful", paste the ID here and move on to the next cell.
# Note: the get_remote(...).download(...) method comes from ECMWF forum discussions, not from the official
# documentation. It works with cdsapi 0.7.7, but it may change in future versions.
REQUEST_ID = "<YOUR_REQUEST_ID>"

# COMMAND ----------

# Step 3: download the result (the ZIP contains two NetCDF files).
import os
import cdsapi

if REQUEST_ID.startswith("<"):
    raise ValueError("Set REQUEST_ID in the previous cell.")

ZIP_PATH = "/tmp/era5_month.zip"
client = cdsapi.Client(url=CDS_URL, key=CDS_KEY, wait_until_complete=False, delete=False)
remote = client.client.get_remote(REQUEST_ID)
remote.download(ZIP_PATH)
print(f"Downloaded: {ZIP_PATH}, {os.path.getsize(ZIP_PATH) / 1024:.1f} KiB")

# COMMAND ----------

# Step 4: extraction and inspection of the two files.
# The "instant" file contains t2m, u100, v100, tcc; the "accum" file contains ssrd (accumulated radiation).
import zipfile
import xarray as xr

DEST = "/tmp/era5_month_extracted"
os.makedirs(DEST, exist_ok=True)
with zipfile.ZipFile(ZIP_PATH) as z:
    print("ZIP content:", z.namelist())
    z.extractall(DEST)

for name in sorted(os.listdir(DEST)):
    path = os.path.join(DEST, name)
    print(f"\nFile: {name}, {os.path.getsize(path) / 1024:.1f} KiB")
    ds = xr.open_dataset(path)
    for v in ds.data_vars:
        a = ds[v]
        print(f"  {v}: units={a.attrs.get('units')}, nulls={int(a.isnull().sum())}, shape={a.shape}")
    t = ds["valid_time"].values
    print(f"  first hour: {t[0]}, last hour: {t[-1]}, hours: {len(t)}")

# COMMAND ----------

# Step 5: quality checks and unit conversions.
# Conversions: t2m from kelvin to °C; wind = sqrt(u^2 + v^2); tcc from fraction (0 to 1) to percentage if needed.
import numpy as np

inst = xr.open_dataset(f"{DEST}/data_stream-oper_stepType-instant.nc")
acc = xr.open_dataset(f"{DEST}/data_stream-oper_stepType-accum.nc")

t = inst["valid_time"].values
print("Constant hourly step:", bool((np.diff(t) == np.timedelta64(1, "h")).all()))

t2m_c = inst["t2m"] - 273.15
wind = np.sqrt(inst["u100"] ** 2 + inst["v100"] ** 2)
print(f"Temperature °C: min {float(t2m_c.min()):.1f}, mean {float(t2m_c.mean()):.1f}, max {float(t2m_c.max()):.1f}")
print(f"Wind at 100 m (m/s): min {float(wind.min()):.1f}, mean {float(wind.mean()):.1f}, max {float(wind.max()):.1f}")
print(f"Cloud cover (fraction): min {float(inst['tcc'].min()):.2f}, max {float(inst['tcc'].max()):.2f}")

# COMMAND ----------

# Step 6: average hourly curve of solar radiation.
# ssrd is in accumulated J/m2; dividing by 3600 gives the hourly mean in W/m2.
# Assumed convention (deduced from the symmetry of the monthly curve): the value with timestamp T
# is the accumulation of the hour ending at T. To be confirmed against the ECMWF documentation.
ssrd = acc["ssrd"].mean(dim=["latitude", "longitude"])
by_hour = ssrd.groupby("valid_time.hour").mean() / 3600
print("Mean radiation per UTC hour (W/m2):")
print(np.round(by_hour.values, 0))

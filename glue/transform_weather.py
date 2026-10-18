import json
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType
from pyspark.sql.window import Window

# ---- 1. Job settings -------------------------------------------------------
optional = [n for n in ("target_date", "STATION") if f"--{n}" in sys.argv]
args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET"] + optional)
BUCKET = args["BUCKET"]
STATION = args.get("STATION", "KCMI")
TARGET_DATE = args.get("target_date") or (
    datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

s3 = boto3.client("s3")

# The unit each measurement should arrive in. A different unit gets the row rejected.
UNITS = {
    "temperature": "wmoUnit:degC",
    "dewpoint": "wmoUnit:degC",
    "relativeHumidity": "wmoUnit:percent",
    "windDirection": "wmoUnit:degree_(angle)",
    "windSpeed": "wmoUnit:km_h-1",
    "windGust": "wmoUnit:km_h-1",
    "barometricPressure": "wmoUnit:Pa",
    "seaLevelPressure": "wmoUnit:Pa",
    "visibility": "wmoUnit:m",
    "precipitationLast3Hours": "wmoUnit:mm",
    "windChill": "wmoUnit:degC",
    "heatIndex": "wmoUnit:degC",
}


# ---- 2. Find the raw files -------------------------------------------------
def list_keys(prefix):
    keys = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        keys += [o["Key"] for o in page.get("Contents", [])]
    return sorted(k for k in keys if k.endswith(".json"))


def num(x):
    return float(x) if x is not None else None


# ---- 3. Turn one JSON file into one flat row -------------------------------
def parse_file(key):
    doc = json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    p = doc.get("properties") or {}
    coords = (doc.get("geometry") or {}).get("coordinates") or [None, None]
    bad_units = []

    def m(name):
        item = p.get(name) or {}
        v = item.get("value")
        if v is None:
            return None
        if item.get("unitCode") != UNITS[name]:
            bad_units.append(name)
        return float(v)

    try:
        observed = int(datetime.fromisoformat(p["timestamp"]).timestamp())
    except Exception:
        observed = None

    layers = p.get("cloudLayers") or []
    first = layers[0] if layers else {}
    return (
        key, p.get("stationId"), p.get("stationName"), observed,
        num(coords[1]), num(coords[0]), num((p.get("elevation") or {}).get("value")),
        p.get("textDescription"), json.dumps(p.get("presentWeather") or []),
        m("temperature"), m("dewpoint"), m("relativeHumidity"), m("windDirection"),
        m("windSpeed"), m("windGust"), m("barometricPressure"), m("seaLevelPressure"),
        m("visibility"), m("precipitationLast3Hours"), m("windChill"), m("heatIndex"),
        first.get("amount"), num((first.get("base") or {}).get("value")),
        ",".join(bad_units),
    )


def D(name):
    return StructField(name, DoubleType())


def S(name):
    return StructField(name, StringType())


SCHEMA = StructType([
    S("source_key"), S("station_id"), S("station_name"), StructField("observed_ts", LongType()),
    D("lat"), D("lon"), D("elevation_m"), S("text_description"), S("present_weather_json"),
    D("temperature_c"), D("dewpoint_c"), D("relative_humidity_pct"), D("wind_direction_deg"),
    D("wind_speed_kmh"), D("wind_gust_kmh"), D("pressure_pa"), D("sea_level_pressure_pa"),
    D("visibility_m"), D("precip_last_3h_mm"), D("wind_chill_c"), D("heat_index_c"),
    S("cloud_amount"), D("cloud_base_m"), S("unit_problem"),
])


def main():
    prefix = f"raw/weather/nws/{STATION}/date={TARGET_DATE}/"
    keys = list_keys(prefix)
    print(f"Found {len(keys)} raw files under {prefix}")
    if not keys:
        return

    with ThreadPoolExecutor(max_workers=8) as pool:
        rows = list(pool.map(parse_file, keys))
    df = spark.createDataFrame(rows, SCHEMA)
    n_raw = df.count()

    # ---- 4. Remove repeats: same station and observation time -------------
    w = Window.partitionBy("station_id", "observed_ts").orderBy("source_key")
    df = df.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")
    df = df.withColumn("observed_time", F.col("observed_ts").cast("timestamp"))

    # ---- 5. Flag bad rows --------------------------------------------------
    reason = (F.when(F.col("observed_ts").isNull(), "missing_timestamp")
               .when(F.col("unit_problem") != "", "unexpected_unit")
               .when(~F.col("temperature_c").between(-60, 55), "temperature_out_of_range")
               .when(~F.col("relative_humidity_pct").between(0, 100), "humidity_out_of_range"))
    df = df.withColumn("reject_reason", reason).withColumn("date", F.lit(TARGET_DATE)).cache()

    good = df.filter(F.col("reject_reason").isNull()).drop("reject_reason", "unit_problem")
    bad = df.filter(F.col("reject_reason").isNotNull())
    n_after, n_good, n_bad = df.count(), good.count(), bad.count()
    n_precip = good.filter(F.col("precip_last_3h_mm").isNotNull()).count()
    conditions = {r["text_description"]: r["count"]
                  for r in good.groupBy("text_description").count().collect()}
    print(f"{TARGET_DATE} weather: {len(keys)} files, {n_raw} raw rows, {n_after} after dedup, "
          f"{n_good} good, {n_bad} rejected, {n_precip} with a precipitation value")
    print("Conditions seen:", conditions)

    # ---- 6. Write Parquet, one file per day --------------------------------
    good.coalesce(1).write.mode("overwrite").partitionBy("date").parquet(
        f"s3://{BUCKET}/processed/weather/nws/")
    if n_bad > 0:
        bad.coalesce(1).write.mode("overwrite").partitionBy("date").parquet(
            f"s3://{BUCKET}/rejected/weather/nws/")
    print("Done")


main()
job.commit()
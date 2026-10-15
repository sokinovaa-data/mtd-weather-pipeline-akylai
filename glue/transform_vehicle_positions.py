import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from google.transit import gtfs_realtime_pb2 as pb
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType
from pyspark.sql.window import Window

# ---- 1. Job settings -------------------------------------------------------
required = ["JOB_NAME", "BUCKET"]
if "--target_date" in sys.argv:
    required.append("target_date")
args = getResolvedOptions(sys.argv, required)

BUCKET = args["BUCKET"]
if "target_date" in args:
    TARGET_DATE = args["target_date"]  # for example 2026-10-13 (UTC)
else:
    TARGET_DATE = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")

# A generous box around Champaign-Urbana. Points outside it are rejected (an assumption to review).
LAT_MIN, LAT_MAX = 39.9, 40.3
LON_MIN, LON_MAX = -88.5, -88.0

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

s3 = boto3.client("s3")


# ---- 2 and 3. Find and decode the raw files --------------------------------
def list_keys(prefix):
    keys = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        keys += [o["Key"] for o in page.get("Contents", [])]
    return [k for k in keys if k.endswith(".pb")]


def decode_file(key):
    feed = pb.FeedMessage()
    feed.ParseFromString(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    out = []
    for e in feed.entity:
        if not e.HasField("vehicle"):
            continue
        v = e.vehicle
        out.append((
            key,
            feed.header.timestamp,
            v.vehicle.id,
            v.trip.trip_id,
            v.trip.route_id,
            float(v.position.latitude),
            float(v.position.longitude),
            pb.VehiclePosition.VehicleStopStatus.Name(v.current_status),
            v.stop_id,
            v.timestamp,
            pb.VehiclePosition.CongestionLevel.Name(v.congestion_level),
            pb.VehiclePosition.OccupancyStatus.Name(v.occupancy_status),
        ))
    return out


SCHEMA = StructType([
    StructField("source_key", StringType()),
    StructField("snapshot_ts", LongType()),
    StructField("vehicle_id", StringType()),
    StructField("trip_id", StringType()),
    StructField("route_id", StringType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("status", StringType()),
    StructField("stop_id", StringType()),
    StructField("vehicle_ts", LongType()),
    StructField("congestion", StringType()),
    StructField("occupancy", StringType()),
])


def main():
    prefix = f"raw/mtd/vehicle_positions/date={TARGET_DATE}/"
    keys = list_keys(prefix)
    print(f"Found {len(keys)} raw files under {prefix}")
    if not keys:
        return

    with ThreadPoolExecutor(max_workers=16) as pool:
        rows = [r for part in pool.map(decode_file, keys) for r in part]
    print(f"Decoded {len(rows)} vehicle records")
    if not rows:
        return

    # ---- 4. Spark table with real timestamps (UTC) -------------------------
    df = spark.createDataFrame(rows, SCHEMA)
    df = (df.withColumn("snapshot_time", F.col("snapshot_ts").cast("timestamp"))
            .withColumn("vehicle_time", F.col("vehicle_ts").cast("timestamp")))

    # ---- 5. Remove duplicates (keep the earliest snapshot) -----------------
    w = Window.partitionBy("vehicle_id", "vehicle_ts").orderBy("snapshot_ts")
    df = df.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")

    # ---- 6. Flag bad rows --------------------------------------------------
    in_area = F.col("lat").between(LAT_MIN, LAT_MAX) & F.col("lon").between(LON_MIN, LON_MAX)
    reason = (F.when(F.col("vehicle_id") == "", "missing_vehicle_id")
               .when(F.col("vehicle_ts") == 0, "missing_vehicle_timestamp")
               .when(~in_area, "position_outside_area"))
    df = df.withColumn("reject_reason", reason).withColumn("date", F.lit(TARGET_DATE)).cache()

    good = df.filter(F.col("reject_reason").isNull()).drop("reject_reason")
    bad = df.filter(F.col("reject_reason").isNotNull())

    n_after, n_good, n_bad = df.count(), good.count(), bad.count()
    print(f"{TARGET_DATE}: {len(rows)} raw rows, {n_after} after dedup, {n_good} good, {n_bad} rejected")

    # ---- 7. Write Parquet, one file per day --------------------------------
    good.coalesce(1).write.mode("overwrite").partitionBy("date").parquet(
        f"s3://{BUCKET}/processed/mtd/vehicle_positions/")
    if n_bad > 0:
        bad.coalesce(1).write.mode("overwrite").partitionBy("date").parquet(
            f"s3://{BUCKET}/rejected/mtd/vehicle_positions/")
    print("Done")


main()
job.commit()
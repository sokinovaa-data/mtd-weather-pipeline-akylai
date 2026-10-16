import sys
from datetime import datetime, timedelta, timezone

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

# ---- 1. Job settings -------------------------------------------------------
required = ["JOB_NAME", "BUCKET"]
if "--target_date" in sys.argv:
    required.append("target_date")
args = getResolvedOptions(sys.argv, required)

BUCKET = args["BUCKET"]
if "target_date" in args:
    TARGET_DATE = args["target_date"]  # for example 2026-10-14 (UTC)
else:
    TARGET_DATE = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")


# ---- 2. Find the raw files (runs on the driver) ----------------------------
def list_keys(prefix):
    s3 = boto3.client("s3")
    keys = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        keys += [o["Key"] for o in page.get("Contents", [])]
    return [k for k in keys if k.endswith(".pb")]


# ---- 3. Decode files (runs in parallel on the Spark workers) ---------------
def decode_partition(keys):
    import boto3
    from google.transit import gtfs_realtime_pb2 as pb

    s3 = boto3.client("s3")
    rel = pb.TripUpdate.StopTimeUpdate.ScheduleRelationship
    for key in keys:
        feed = pb.FeedMessage()
        feed.ParseFromString(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
        snap = feed.header.timestamp
        for e in feed.entity:
            if not e.HasField("trip_update"):
                continue
            tu = e.trip_update
            vid = tu.vehicle.id if tu.HasField("vehicle") else ""
            for s in tu.stop_time_update:
                a, d = s.arrival, s.departure
                yield (
                    key, snap, tu.trip.trip_id, tu.trip.route_id,
                    tu.trip.start_date, tu.trip.start_time, vid,
                    s.stop_sequence, s.stop_id,
                    a.time if s.HasField("arrival") and a.HasField("time") else None,
                    d.time if s.HasField("departure") and d.HasField("time") else None,
                    a.delay if s.HasField("arrival") and a.HasField("delay") else None,
                    d.delay if s.HasField("departure") and d.HasField("delay") else None,
                    rel.Name(s.schedule_relationship),
                )


SCHEMA = StructType([
    StructField("source_key", StringType()),
    StructField("snapshot_ts", LongType()),
    StructField("trip_id", StringType()),
    StructField("route_id", StringType()),
    StructField("start_date", StringType()),
    StructField("start_time", StringType()),
    StructField("vehicle_id", StringType()),
    StructField("stop_sequence", IntegerType()),
    StructField("stop_id", StringType()),
    StructField("arrival_ts", LongType()),
    StructField("departure_ts", LongType()),
    StructField("arrival_delay", IntegerType()),
    StructField("departure_delay", IntegerType()),
    StructField("schedule_relationship", StringType()),
])


def main():
    prefix = f"raw/mtd/trip_updates/date={TARGET_DATE}/"
    keys = list_keys(prefix)
    print(f"Found {len(keys)} raw files under {prefix}")
    if not keys:
        return

    rows = sc.parallelize(keys, max(1, len(keys) // 25)).mapPartitions(decode_partition)
    df = spark.createDataFrame(rows, SCHEMA).cache()
    n_raw = df.count()
    print(f"Decoded {n_raw} stop predictions")
    if n_raw == 0:
        return

    # ---- 4. Remove exact repeats -------------------------------------------
    df = df.dropDuplicates(["trip_id", "start_date", "stop_sequence", "snapshot_ts"])

    # ---- 5. Real timestamps (UTC) and a service date -----------------------
    df = (df.withColumn("snapshot_time", F.col("snapshot_ts").cast("timestamp"))
            .withColumn("arrival_time", F.col("arrival_ts").cast("timestamp"))
            .withColumn("departure_time", F.col("departure_ts").cast("timestamp"))
            .withColumn("service_date", F.to_date(F.col("start_date"), "yyyyMMdd")))

    # ---- 6. Flag bad rows --------------------------------------------------
    predicted = F.coalesce(F.col("arrival_ts"), F.col("departure_ts"))
    reason = (F.when(F.col("trip_id") == "", "missing_trip_id")
               .when(F.col("stop_id") == "", "missing_stop_id")
               .when(predicted.isNull(), "missing_prediction_time")
               .when(~(predicted - F.col("snapshot_ts")).between(-43200, 86400), "prediction_out_of_range"))
    df = df.withColumn("reject_reason", reason).withColumn("date", F.lit(TARGET_DATE)).cache()

    good = df.filter(F.col("reject_reason").isNull()).drop("reject_reason")
    bad = df.filter(F.col("reject_reason").isNotNull())
    n_after, n_good, n_bad = df.count(), good.count(), bad.count()
    n_delay = good.filter(F.col("arrival_delay").isNotNull() | F.col("departure_delay").isNotNull()).count()
    print(f"{TARGET_DATE}: {n_raw} raw rows, {n_after} after dedup, {n_good} good, {n_bad} rejected, "
          f"{n_delay} with a delay field")

    # ---- 7. Write Parquet, one file per day --------------------------------
    good.coalesce(1).write.mode("overwrite").partitionBy("date").parquet(
        f"s3://{BUCKET}/processed/mtd/trip_updates/")
    if n_bad > 0:
        bad.coalesce(1).write.mode("overwrite").partitionBy("date").parquet(
            f"s3://{BUCKET}/rejected/mtd/trip_updates/")
    print("Done")


main()
job.commit()
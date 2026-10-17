import csv
import io
import re
import sys
import zipfile

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

# ---- 1. Job settings -------------------------------------------------------
required = ["JOB_NAME", "BUCKET"]
if "--feed_date" in sys.argv:
    required.append("feed_date")
args = getResolvedOptions(sys.argv, required)
BUCKET = args["BUCKET"]

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

s3 = boto3.client("s3")

# Column types to apply on top of "everything is text"
INT_COLS = {
    "routes": ["route_type"],
    "trips": ["direction_id", "wheelchair_accessible", "bikes_allowed"],
    "stops": ["location_type", "wheelchair_boarding"],
    "stop_times": ["stop_sequence", "pickup_type", "drop_off_type", "timepoint"],
    "calendar": ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"],
    "calendar_dates": ["exception_type"],
}
DOUBLE_COLS = {"stops": ["stop_lat", "stop_lon"]}
DATE_COLS = {"calendar": ["start_date", "end_date"], "calendar_dates": ["date"]}
TABLES = ["routes", "trips", "stops", "stop_times", "calendar", "calendar_dates", "feed_info"]


# ---- 2. Pick the zip to process --------------------------------------------
def pick_zip():
    keys = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix="raw/mtd/static_gtfs/"):
        keys += [o["Key"] for o in page.get("Contents", [])]
    keys = sorted(k for k in keys if k.endswith(".zip"))
    if "feed_date" in args:
        keys = [k for k in keys if f"date={args['feed_date']}/" in k]
    return keys[-1] if keys else None


def to_secs(col):
    p = F.split(F.col(col), ":")
    return (p.getItem(0).cast("int") * 3600 + p.getItem(1).cast("int") * 60 + p.getItem(2).cast("int"))


def read_table(z, name):
    text = z.read(f"{name}.txt").decode("utf-8-sig")
    rows = list(csv.reader(io.StringIO(text)))
    header, data = rows[0], rows[1:]
    n = len(header)
    data = [(r + [""] * n)[:n] for r in data]  # pad or trim ragged rows
    schema = StructType([StructField(c, StringType()) for c in header])
    df = spark.createDataFrame(data, schema)
    for c in header:  # empty text becomes null
        df = df.withColumn(c, F.when(F.trim(F.col(c)) == "", None).otherwise(F.col(c)))
    return df


def main():
    key = pick_zip()
    if not key:
        print("No zip found under raw/mtd/static_gtfs/")
        return
    version = re.search(r"date=(\d{4}-\d{2}-\d{2})", key).group(1)
    print(f"Processing {key} as feed_version {version}")

    z = zipfile.ZipFile(io.BytesIO(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()))
    summary = []

    for name in TABLES:
        if f"{name}.txt" not in z.namelist():
            print(f"Skipping {name}: not in the zip")
            continue
        df = read_table(z, name)
        cols = df.columns

        for c in INT_COLS.get(name, []):
            if c in cols:
                df = df.withColumn(c, F.col(c).cast("int"))
        for c in DOUBLE_COLS.get(name, []):
            if c in cols:
                df = df.withColumn(c, F.col(c).cast("double"))
        for c in DATE_COLS.get(name, []):
            if c in cols:
                df = df.withColumn(c, F.to_date(F.col(c), "yyyyMMdd"))
        if name == "calendar_dates":
            df = df.withColumnRenamed("date", "exception_date")
        if name == "stop_times":
            df = (df.withColumn("arrival_secs", to_secs("arrival_time"))
                    .withColumn("departure_secs", to_secs("departure_time")))

        df = df.withColumn("feed_version", F.lit(version))
        n = df.count()
        df.coalesce(1).write.mode("overwrite").partitionBy("feed_version").parquet(
            f"s3://{BUCKET}/processed/mtd/static_gtfs/{name}/")
        summary.append(f"{name}={n}")

    print(f"feed_version={version} rows: " + ", ".join(summary))
    print("Done")


main()
job.commit()
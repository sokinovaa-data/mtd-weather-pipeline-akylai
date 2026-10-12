import os
import urllib.request
from datetime import datetime, timezone

import boto3

s3 = boto3.client("s3")
BUCKET = os.environ["BUCKET"]
FEED_URL = os.environ["FEED_URL"]
FEED_NAME = os.environ.get("FEED_NAME", "vehicle_positions")


def lambda_handler(event, context):
    req = urllib.request.Request(FEED_URL, headers={"User-Agent": "mtd-weather-pipeline"})
    with urllib.request.urlopen(req, timeout=20) as resp:
        body = resp.read()

    now = datetime.now(timezone.utc)
    key = (
        f"raw/mtd/{FEED_NAME}/date={now:%Y-%m-%d}/hour={now:%H}/"
        f"{now:%Y%m%dT%H%M%SZ}.pb"
    )
    s3.put_object(Bucket=BUCKET, Key=key, Body=body)

    print(f"Saved {len(body)} bytes to s3://{BUCKET}/{key}")
    return {"status": "ok", "key": key, "bytes": len(body)}
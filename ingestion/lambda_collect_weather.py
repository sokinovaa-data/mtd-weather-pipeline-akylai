import os
import urllib.request
from datetime import datetime, timezone

import boto3

s3 = boto3.client("s3")
BUCKET = os.environ["BUCKET"]
STATION = os.environ["STATION"]
USER_AGENT = os.environ["USER_AGENT"]


def lambda_handler(event, context):
    url = f"https://api.weather.gov/stations/{STATION}/observations/latest"
    req = urllib.request.Request(
        url, headers={"User-Agent": USER_AGENT, "Accept": "application/geo+json"}
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        body = resp.read()

    now = datetime.now(timezone.utc)
    key = (
        f"raw/weather/nws/{STATION}/date={now:%Y-%m-%d}/hour={now:%H}/"
        f"{now:%Y%m%dT%H%M%SZ}.json"
    )
    s3.put_object(Bucket=BUCKET, Key=key, Body=body)

    print(f"Saved {len(body)} bytes to s3://{BUCKET}/{key}")
    return {"status": "ok", "key": key, "bytes": len(body)}
import hashlib
import json
import os
import urllib.request
from datetime import datetime, timezone

import boto3

s3 = boto3.client("s3")
BUCKET = os.environ["BUCKET"]
GTFS_URL = os.environ["GTFS_URL"]
STATE_KEY = "state/static_gtfs_last.json"


def load_state():
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=STATE_KEY)
        return json.loads(obj["Body"].read())
    except s3.exceptions.NoSuchKey:
        return {}


def lambda_handler(event, context):
    req = urllib.request.Request(GTFS_URL, headers={"User-Agent": "mtd-weather-pipeline"})
    with urllib.request.urlopen(req, timeout=50) as resp:
        body = resp.read()
        etag = resp.headers.get("ETag")
        last_modified = resp.headers.get("Last-Modified")

    digest = hashlib.sha256(body).hexdigest()
    state = load_state()
    if state.get("sha256") == digest:
        print("No change, nothing saved")
        return {"status": "unchanged"}

    now = datetime.now(timezone.utc)
    key = f"raw/mtd/static_gtfs/date={now:%Y-%m-%d}/gtfs_{now:%Y%m%dT%H%M%SZ}.zip"
    s3.put_object(Bucket=BUCKET, Key=key, Body=body)

    new_state = {
        "sha256": digest,
        "etag": etag,
        "last_modified": last_modified,
        "key": key,
        "saved_at": now.isoformat(),
    }
    s3.put_object(Bucket=BUCKET, Key=STATE_KEY, Body=json.dumps(new_state).encode())
    print(f"New version saved to s3://{BUCKET}/{key}")
    return {"status": "saved", "key": key}

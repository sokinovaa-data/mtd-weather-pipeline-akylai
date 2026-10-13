import requests
from google.transit import gtfs_realtime_pb2
import time


FEEDS = {
    "vehicle_positions": "https://gtfs-rt.mtd.org/vehicle-positions",
    "trip_updates": "https://gtfs-rt.mtd.org/trip-updates",
    "service_alerts": "https://gtfs-rt.mtd.org/service-alerts",
}

for name, url in FEEDS.items():
    print(f"\n=== {name} ===")
    response = requests.get(url, timeout=30)
    print("Status:", response.status_code)
    print("Content-Type:", response.headers.get("Content-Type"))
    response.raise_for_status()
    print(f"Size: {len(response.content) / 1024:.1f} KB")

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)

    print("Feed timestamp:", feed.header.timestamp)
    print("Entities:", len(feed.entity))
    print("vehicles:", sum(1 for e in feed.entity if e.HasField("vehicle")))
    print("trip updates:", sum(1 for e in feed.entity if e.HasField("trip_update")))
    print("alerts:", sum(1 for e in feed.entity if e.HasField("alert")))
    if feed.entity:
        print(feed.entity[0])
        from datetime import datetime, timezone

        print("Readable time (UTC):", datetime.fromtimestamp(feed.header.timestamp, tz=timezone.utc))

        from collections import Counter

        counts = Counter()
        for e in feed.entity:
            if e.HasField("vehicle"):
                for field, _ in e.vehicle.ListFields():
                    counts[field.name] += 1
        if counts:
            print("Vehicle fields populated (out of", len(feed.entity), "):", dict(counts))
            if counts:
                print("Vehicle fields populated (out of", len(feed.entity), "):", dict(counts))

        # NEW: age of vehicle records (indent 4 spaces, same level as "if feed.entity:")
        vts = [e.vehicle.timestamp for e in feed.entity if e.HasField("vehicle") and e.vehicle.timestamp]
        if vts:
            now = time.time()
            print("Vehicle age in seconds: newest", round(now - max(vts)), "oldest", round(now - min(vts)))
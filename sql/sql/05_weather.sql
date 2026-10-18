# What the data contains (updated Oct 17, 2026)

## Collection windows (UTC)
- Vehicle positions: from 2026-10-12 00:12 (first partial day)
- Trip updates: from 2026-10-13 00:14
- Static GTFS: first copy saved 2026-10-14 00:18
- Weather: hourly, from 2026-10-13
- Schedules: vehicle every 1 min, trip updates every 2 min, service alerts every 10 min,
  weather every hour, static GTFS checked daily

## MTD vehicle positions (GTFS-Realtime, protobuf)
- No API key. Feed refreshes at least every 20 seconds
- About 3 KB per snapshot
- Fields populated: trip, position (lat/lon only, no speed or bearing seen), current_status,
  timestamp, congestion_level, stop_id, vehicle, occupancy_status
- 2026-10-14: 64,604 raw rows, 60,997 after dedup (5.6% duplicates), 0 rejected
- Dedup key: vehicle_id + the vehicle's own timestamp

## MTD trip updates
- About 111 KB per snapshot; one row per predicted stop per trip
- About 2.95 to 2.99 million predictions per day, about 1,850 trips, 1,649 stops
- Predictions cover the rest of the trip, not just the next stop
  (median 2,303 s ahead, p90 4,583 s, on one day)
- Delay field: to verify (COUNT(arrival_delay) in Athena)

## MTD service alerts
- About 2 to 3 KB per snapshot; includes a long-running construction closure (Broadway)

## MTD static GTFS (schedule)
- https://mtd.dev/gtfs.zip, about 2.35 MB; ETag and Last-Modified provided
- Valid 2026-08-09 to 2026-12-19, so a new version is expected
- Tables: routes (90), trips (5,348), stops (1,921), stop_times (196,102), calendar_dates (12,395)
- Kept one version per save date. Times can pass 24:00
- 2026-10-14: 1,851 of 1,851 realtime trips matched the schedule

## Weather (NWS, api.weather.gov, station KCMI)
- No key; a User-Agent header is required
- About 4 KB per file. Every value carries a unit (degC, km/h, Pa, m, percent)
- Fields: temperature, dewpoint, humidity, wind speed/direction/gust, pressure, visibility,
  text conditions, cloud layers
- Precipitation: only precipitationLast3Hours exists, and it was null in a clear-sky file.
  To retest in rain
- Observation time can lag the fetch by about 25 minutes, so dedup on observation time
- Limitation: one station for the whole bus network

## Time handling
- All stored timestamps are UTC. Local (America/Chicago) conversion happens only at query time
- Daylight saving ends 2026-11-01, so recheck any local-time output after that date

## Decisions
- Raw files stored untouched in S3 (versioning on, lifecycle rule for old versions)
- Daily Glue Spark jobs write partitioned Parquet to processed/ and bad rows to rejected/
- Lambda collectors; Glue only for the transform
CREATE EXTERNAL TABLE IF NOT EXISTS mtd.trip_updates (
  source_key string, snapshot_ts bigint, trip_id string, route_id string,
  start_date string, start_time string, vehicle_id string, stop_sequence int,
  stop_id string, arrival_ts bigint, departure_ts bigint,
  arrival_delay int, departure_delay int, schedule_relationship string,
  snapshot_time timestamp, arrival_time timestamp, departure_time timestamp,
  service_date date
)
PARTITIONED BY (`date` string)
STORED AS PARQUET
LOCATION 's3://mtd-weather-pipeline-akylai-2026/processed/mtd/trip_updates/';

MSCK REPAIR TABLE mtd.trip_updates;

-- Rows, trips, stops, and files per day
SELECT "date", COUNT(*) AS n_rows, COUNT(DISTINCT trip_id) AS trips,
       COUNT(DISTINCT stop_id) AS stops, COUNT(DISTINCT source_key) AS files
FROM mtd.trip_updates GROUP BY "date" ORDER BY "date";

-- Does MTD ever fill in delay?
SELECT COUNT(*) AS predictions, COUNT(arrival_delay) AS with_arrival_delay,
       COUNT(departure_delay) AS with_departure_delay
FROM mtd.trip_updates WHERE "date" = '2026-10-14';

-- How far ahead do predictions look?
SELECT approx_percentile(arrival_ts - snapshot_ts, 0.5) AS median_seconds_ahead,
       approx_percentile(arrival_ts - snapshot_ts, 0.9) AS p90_seconds_ahead
FROM mtd.trip_updates WHERE "date" = '2026-10-14' AND arrival_ts IS NOT NULL;

-- Prediction counts by horizon
SELECT CASE WHEN arrival_ts - snapshot_ts < 0 THEN 'already past'
            WHEN arrival_ts - snapshot_ts < 300 THEN '0-5 min'
            WHEN arrival_ts - snapshot_ts < 900 THEN '5-15 min'
            WHEN arrival_ts - snapshot_ts < 1800 THEN '15-30 min'
            WHEN arrival_ts - snapshot_ts < 3600 THEN '30-60 min'
            ELSE '60+ min' END AS horizon,
       COUNT(*) AS predictions
FROM mtd.trip_updates WHERE "date" = '2026-10-14' AND arrival_ts IS NOT NULL
GROUP BY 1 ORDER BY MIN(arrival_ts - snapshot_ts);
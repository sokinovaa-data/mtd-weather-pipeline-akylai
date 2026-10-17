-- Table over the Glue job's output
CREATE EXTERNAL TABLE IF NOT EXISTS mtd.vehicle_positions (
  source_key string, snapshot_ts bigint, vehicle_id string, trip_id string,
  route_id string, lat double, lon double, status string, stop_id string,
  vehicle_ts bigint, congestion string, occupancy string,
  snapshot_time timestamp, vehicle_time timestamp
)
PARTITIONED BY (`date` string)
STORED AS PARQUET
LOCATION 's3://mtd-weather-pipeline-akylai-2026/processed/mtd/vehicle_positions/';

-- Register new date folders (rerun after each Glue run)
MSCK REPAIR TABLE mtd.vehicle_positions;

-- Rows and buses per day
SELECT "date", COUNT(*) AS n_rows, COUNT(DISTINCT vehicle_id) AS buses
FROM mtd.vehicle_positions GROUP BY "date" ORDER BY "date";

-- Are positions inside the expected area?
SELECT MIN(lat), MAX(lat), MIN(lon), MAX(lon)
FROM mtd.vehicle_positions WHERE "date" = '2026-10-14';

-- How often does each bus report?
WITH g AS (
  SELECT vehicle_id,
         vehicle_ts - LAG(vehicle_ts) OVER (PARTITION BY vehicle_id ORDER BY vehicle_ts) AS gap_s
  FROM mtd.vehicle_positions WHERE "date" = '2026-10-14'
)
SELECT approx_percentile(gap_s, 0.5) AS median_gap_s, approx_percentile(gap_s, 0.9) AS p90_gap_s
FROM g WHERE gap_s IS NOT NULL;

-- Buses on the road per hour, Champaign time
SELECT date_trunc('hour', at_timezone(with_timezone(vehicle_time, 'UTC'), 'America/Chicago')) AS local_hour,
       COUNT(DISTINCT vehicle_id) AS buses
FROM mtd.vehicle_positions WHERE "date" = '2026-10-14'
GROUP BY 1 ORDER BY 1;
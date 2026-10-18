CREATE EXTERNAL TABLE IF NOT EXISTS mtd.gtfs_routes (
  route_id string, route_short_name string, route_long_name string, route_type int, route_color string
) PARTITIONED BY (feed_version string) STORED AS PARQUET
LOCATION 's3://mtd-weather-pipeline-akylai-2026/processed/mtd/static_gtfs/routes/';

CREATE EXTERNAL TABLE IF NOT EXISTS mtd.gtfs_trips (
  route_id string, service_id string, trip_id string, trip_headsign string, direction_id int
) PARTITIONED BY (feed_version string) STORED AS PARQUET
LOCATION 's3://mtd-weather-pipeline-akylai-2026/processed/mtd/static_gtfs/trips/';

CREATE EXTERNAL TABLE IF NOT EXISTS mtd.gtfs_stops (
  stop_id string, stop_name string, stop_lat double, stop_lon double
) PARTITIONED BY (feed_version string) STORED AS PARQUET
LOCATION 's3://mtd-weather-pipeline-akylai-2026/processed/mtd/static_gtfs/stops/';

CREATE EXTERNAL TABLE IF NOT EXISTS mtd.gtfs_stop_times (
  trip_id string, stop_id string, stop_sequence int, arrival_time string,
  departure_time string, arrival_secs int, departure_secs int, timepoint int
) PARTITIONED BY (feed_version string) STORED AS PARQUET
LOCATION 's3://mtd-weather-pipeline-akylai-2026/processed/mtd/static_gtfs/stop_times/';

CREATE EXTERNAL TABLE IF NOT EXISTS mtd.gtfs_calendar (
  service_id string, monday int, tuesday int, wednesday int, thursday int,
  friday int, saturday int, sunday int, start_date date, end_date date
) PARTITIONED BY (feed_version string) STORED AS PARQUET
LOCATION 's3://mtd-weather-pipeline-akylai-2026/processed/mtd/static_gtfs/calendar/';

CREATE EXTERNAL TABLE IF NOT EXISTS mtd.gtfs_calendar_dates (
  service_id string, exception_date date, exception_type int
) PARTITIONED BY (feed_version string) STORED AS PARQUET
LOCATION 's3://mtd-weather-pipeline-akylai-2026/processed/mtd/static_gtfs/calendar_dates/';

-- Register the schedule version in each table (one statement per table)
MSCK REPAIR TABLE mtd.gtfs_routes;
-- repeat for gtfs_trips, gtfs_stops, gtfs_stop_times, gtfs_calendar, gtfs_calendar_dates

-- Do realtime trip IDs exist in the schedule?
WITH rt AS (SELECT DISTINCT trip_id FROM mtd.trip_updates WHERE "date" = '2026-10-14')
SELECT COUNT(*) AS realtime_trips, COUNT(s.trip_id) AS found_in_schedule
FROM rt LEFT JOIN mtd.gtfs_trips s ON rt.trip_id = s.trip_id;

-- Notes for myself!!!
-- After a new schedule is processed, register it in all six tables.
-- From CloudShell, run the for-loop of MSCK REPAIR statements, or run each
-- statement separately in the Athena editor.
CREATE TABLE trip_updates (
  source_key          VARCHAR(500),
  snapshot_ts         BIGINT,
  trip_id             VARCHAR(100),
  route_id            VARCHAR(50),
  start_date          VARCHAR(20),
  start_time          VARCHAR(20),
  vehicle_id          VARCHAR(50),
  stop_sequence       INTEGER,
  stop_id             VARCHAR(50),
  arrival_ts          BIGINT,
  departure_ts        BIGINT,
  arrival_delay       INTEGER,
  departure_delay     INTEGER,
  schedule_relationship VARCHAR(50),
  snapshot_time       TIMESTAMP,
  arrival_time        TIMESTAMP,
  departure_time      TIMESTAMP,
  service_date        DATE
)
SORTKEY (service_date);

COPY trip_updates
FROM 's3://mtd-weather-pipeline-akylai-2026/processed/mtd/trip_updates/date=2026-10-14/'
IAM_ROLE default
FORMAT AS PARQUET;

--check
SELECT trip_id, route_id, stop_id, stop_sequence,
       snapshot_time, arrival_time, service_date,
       arrival_delay, departure_delay
FROM trip_updates
LIMIT 10;

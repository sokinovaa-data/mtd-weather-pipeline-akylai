# mtd-weather-pipeline-akylai
![Python](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-Athena-4479A1?logo=amazonathena&logoColor=white)
![Apache Spark](https://img.shields.io/badge/PySpark-E25A1C?logo=apachespark&logoColor=white)
![AWS](https://img.shields.io/badge/AWS-232F3E?logo=amazonwebservices&logoColor=white)
![Static Badge](https://img.shields.io/badge/aws--EventBridge-red)
![Lambda](https://img.shields.io/badge/AWS_Lambda-FF9900?logo=awslambda&logoColor=white)
![S3](https://img.shields.io/badge/Amazon_S3-569A31?logo=amazons3&logoColor=white)
![Glue](https://img.shields.io/badge/AWS_Glue-8C4FFF?logo=amazonwebservices&logoColor=white)
![Parquet](https://img.shields.io/badge/Apache_Parquet-50ABF1?logo=apache&logoColor=white)

MTD Transit Data Pipeline collects live bus data, the static schedule, and weather data for Champaign-Urbana and turns them into clean, queryable Parquet files on AWS. Five Lambda functions poll the sources every 1 minute to once a day and save the raw files to S3. A daily AWS Glue batch then cleans them into a processed layer, which is queried in place with Athena.

The pipeline handles 5 sources, about 2,300 collector runs a day, and about 3 million processed rows a day.

Phase 2: The pipeline also loads the processed data into Redshift Serverless. (See the Phase 2 section).

## Repository structure

```text
├── architecture
│   └── .gitkeep
├── docs
│   ├── what_the_data_contains.md
│   └── .gitkeep
├── glue
│   ├── .gitkeep
│   ├── transform_static_gtfs.py
│   ├── transform_trip_updates.py
│   ├── transform_vehicle_positions.py
│   └── transform_weather.py
├── ingestion
│   ├── .gitkeep
│   ├── explore_static_gtfs.py
│   ├── fetch_feed.py
│   ├── fetch_weather.py
│   ├── lambda_check_static_gtfs.py
│   ├── lambda_collect_feed.py
│   └── lambda_collect_weather.py
├── sample_data
│   ├── .gitkeep
│   └── weather_sample.json
├── sql
│   ├── sql
│   ├── .gitkeep
│   ├── 02_trip_updates.sql
│   ├── 03_static_gtfs.sql
│   └── transform_static_gtfs.sql
├── .gitignore
├── LICENSE
└── README.md
```
## Tech stack

Python, SQL, PySpark, AWS Lambda, EventBridge, S3, Glue, Athena, CloudWatch, SNS, IAM, Parquet, GTFS-Realtime (protobuf), Redshift



## Architecture diagram
<img width="800" height="1100" alt="photo_5409082152344821483_y" src="https://github.com/user-attachments/assets/0702d933-96a8-4682-a174-e167e4e941c1" />



1. Collect: EventBridge triggers 5 Lambda functions on schedule to collect the data from
sources:
.\
Source1:MTD Bus Schedules https://developer.cumtd.com/ .\
Source2: https://api.weather.gov/ 

(every 1 minute, 2 minutes, 10 minutes, 1 hour, and 1 day) and saves them as raw files in an S3 bucket.

2. Transform: AWS Glue performs daily batch processing: it reads the raw data files, transforms them,
and saves them as processed or rejected files.

3. Query: Athena makes it available to run  SQL on the processed Parquet files in place, using the table
 definitions stored in the Glue Data Catalog.

<img width="699" height="226" alt="monitoring_diagram" src="https://github.com/user-attachments/assets/90434410-162a-4658-82ff-ffe998fb624d" />



Data Models
``` text
Realtime feeds (GTFS-Realtime, protobuf)
1. Vehicle positions
Entity: one bus position report, about 61,000 rows a day.

Fields: vehicle_id, trip_id, route_id, lat, lon, status, stop_id, congestion, occupancy, vehicle_time, snapshot_time, source_key.
Unique row: vehicle_id + vehicle_time.
Joins: trip_id to trips, route_id to routes, stop_id to stops.

2. Trip updates
Entity: one predicted stop for one trip in one snapshot, about 3 million rows a day.

Fields: trip_id, route_id, vehicle_id, stop_id, stop_sequence, arrival_time, departure_time, start_date, service_date, snapshot_time. The delay columns are empty because MTD sends no delay.
Unique row: trip_id + start_date + stop_sequence + snapshot_time.
Joins: trip_id to trips. trip_id + stop_sequence to stop_times.

3. Service alerts
Entity: one announcement, such as a closure or detour.
Fields in the feed: alert ID, active period, affected route IDs, cause, effect, URL, header and description text.
```

```text
MTD static schedule (GTFS, zip of CSV files)
Entities: routes (90 rows), trips (5,348), stops (1,921), stop_times (196,102), calendar (312), calendar_dates (12,395).

Fields: route_id, trip_id, stop_id, service_id, names, coordinates, scheduled times. arrival_secs stores each time as seconds after midnight, so times past 24:00 work.
Joins: trips to routes. stop_times to trips and stops. trips.service_id to calendar and calendar_dates.
Versions: each saved copy gets a feed_version. The first is 2026-10-14, and the feed is valid through 2026-12-19.
```


``` text
NWS weather (station KCMI, JSON)

Fields: station_id, observed_time, temperature_c, dewpoint_c, relative_humidity_pct, wind_speed_kmh, wind_direction_deg, pressure_pa, visibility_m, text_description, precip_last_3h_mm.
Unique row: station_id + observed_time.
Joins: none by key. Weather joins the bus data by UTC hour.
```
Storage choice .\
raw/: native formats, grouped by date and hour, never edited.
processed/: Parquet partitioned by date (feed_version for the schedule), one file per day.
rejected/: rows that failed validation, with a reason.
Nine table definitions in the Glue Data Catalog database mtd.
Retention
Raw and processed files are kept. A rerun replaces that day’s processed file.
S3 versioning is on, and a lifecycle rule removes old versions.
Lambda logs expire after 14 days.
```








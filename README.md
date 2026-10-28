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

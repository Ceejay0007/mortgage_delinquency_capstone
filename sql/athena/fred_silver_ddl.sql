-- Register the FRED Silver Parquet files as a queryable Athena table.

CREATE DATABASE IF NOT EXISTS mortgage_silver;

CREATE EXTERNAL TABLE IF NOT EXISTS mortgage_silver.fred_observations (
  series_id        string,
  measure          string,
  geo_level        string,
  state_code       string,
  frequency        string,
  observation_date date,
  value            double,
  is_missing       boolean,
  ingest_date      date,
  processed_at     timestamp
)
STORED AS PARQUET
LOCATION 's3://conrad-mortgage-capstone-2026/silver/fred_observations/';
{{ config(materialized='view', schema=env_var('DBT_SILVER', 'silver')) }}

WITH base AS (
  SELECT
    sensor_id,
    location_id,
    INITCAP(location_name) AS location_name,
    latitude,
    longitude,
    value AS pm25_value,
    unit,
    datetime_utc,
    DATE(datetime_utc) AS measurement_date
  FROM `{{ env_var('PROJECT', env_var('GOOGLE_CLOUD_PROJECT')) }}.{{ env_var('DBT_RAW', 'raw') }}.openaq_pm25_ca`
  WHERE datetime_utc IS NOT NULL
)

SELECT
  sensor_id,
  location_id,
  location_name,
  latitude,
  longitude,
  pm25_value,
  unit,
  datetime_utc,
  measurement_date
FROM base

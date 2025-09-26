{{ config(materialized='view', schema=env_var('DBT_GOLD', 'gold')) }}

WITH air_quality AS (
  SELECT *
  FROM `{{ env_var('PROJECT', env_var('GOOGLE_CLOUD_PROJECT')) }}.{{ env_var('DBT_SILVER', 'silver') }}`.openaq_pm25_daily
), weather AS (
  SELECT *
  FROM `{{ env_var('PROJECT', env_var('GOOGLE_CLOUD_PROJECT')) }}.{{ env_var('DBT_SILVER', 'silver') }}`.weather_ca_daily
)

SELECT
  a.measurement_date,
  a.location_id,
  a.location_name,
  a.latitude,
  a.longitude,
  a.pm25_avg,
  a.pm25_max,
  a.pm25_min,
  a.unit,
  w.temp_mean,
  w.temp_max,
  w.temp_min
FROM air_quality a
LEFT JOIN weather w
  ON w.date = a.measurement_date

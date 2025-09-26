{{ config(materialized='view', schema=env_var('DBT_SILVER', 'silver')) }}

WITH measurements AS (
  SELECT *
  FROM {{ ref('openaq_pm25_measurements') }}
)

SELECT
  measurement_date,
  location_id,
  ANY_VALUE(location_name) AS location_name,
  ANY_VALUE(latitude) AS latitude,
  ANY_VALUE(longitude) AS longitude,
  AVG(pm25_value) AS pm25_avg,
  MAX(pm25_value) AS pm25_max,
  MIN(pm25_value) AS pm25_min,
  ANY_VALUE(unit) AS unit
FROM measurements
GROUP BY measurement_date, location_id

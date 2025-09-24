-- statewide daily averages (view) from raw point-level table
{{ config(materialized='view') }}

SELECT
  DATE(date) AS date,
  'ca'       AS region,
  AVG(temp_mean) AS temp_mean,
  AVG(temp_max)  AS temp_max,
  AVG(temp_min)  AS temp_min
FROM `project-starfishprime-001.silver.weather_points_daily`
GROUP BY date

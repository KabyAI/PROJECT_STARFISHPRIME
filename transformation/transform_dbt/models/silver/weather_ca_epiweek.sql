{{ config(materialized='view') }}

WITH d AS (
  SELECT
    DATE(date) AS dte,
    region,
    temp_mean,
    temp_max,
    temp_min
  FROM `{{ env_var('PROJECT', var('project', target.project)) }}.{{ env_var('DBT_SILVER','silver') }}.weather_ca_daily`
  WHERE region = 'ca'
)
SELECT
  'ca' AS region,
  e.epiweek,
  AVG(d.temp_mean) AS temp_mean_epiwk,
  AVG(d.temp_max)  AS temp_max_epiwk,
  AVG(d.temp_min)  AS temp_min_epiwk
FROM d
JOIN {{ ref('dim_epiweeks') }} e
  ON e.date = d.dte
GROUP BY region, epiweek

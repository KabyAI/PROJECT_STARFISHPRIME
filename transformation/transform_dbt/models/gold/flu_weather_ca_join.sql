{{ config(materialized='view', schema=env_var('DBT_GOLD','gold')) }}

WITH flu AS (
  SELECT *
  FROM `{{ env_var('PROJECT', env_var('GOOGLE_CLOUD_PROJECT')) }}.{{ env_var('DBT_SILVER','silver') }}`.fluview_ca_weekly
), weather AS (
  SELECT *
  FROM `{{ env_var('PROJECT', env_var('GOOGLE_CLOUD_PROJECT')) }}.{{ env_var('DBT_SILVER','silver') }}`.weather_ca_epiweek
)

SELECT
  f.region,
  f.epiweek,
  f.week_start,
  f.wili,
  f.ili,
  f.num_ili,
  f.num_patients,
  f.release_date,
  w.temp_mean_epiwk,
  w.temp_max_epiwk,
  w.temp_min_epiwk
FROM flu f
LEFT JOIN weather w
  ON w.region = f.region
 AND w.epiweek = f.epiweek
WHERE f.region IN ('ca', 'state:ca')

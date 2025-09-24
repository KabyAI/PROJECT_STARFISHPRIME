{{ config(materialized='view') }}

WITH f AS (
  SELECT region, epiweek, wili, ili, num_ili, num_patients, release_date
  FROM `{{ env_var('PROJECT', var('project', target.project)) }}.{{ env_var('DBT_SILVER','silver') }}.fluview_test`
  WHERE region IN ('ca','state:ca')
),
w AS (
  SELECT region, epiweek, temp_mean_epiwk, temp_max_epiwk, temp_min_epiwk
  FROM {{ ref('weather_ca_epiweek') }}
)
SELECT
  f.region,
  f.epiweek,
  f.wili,
  f.ili,
  f.num_ili,
  f.num_patients,
  f.release_date,
  w.temp_mean_epiwk,
  w.temp_max_epiwk,
  w.temp_min_epiwk
FROM f
LEFT JOIN w
  ON w.region = 'ca' AND w.epiweek = f.epiweek

{{ config(materialized='view', schema=env_var('DBT_SILVER', 'silver')) }}

{% set region_label = env_var('DBT_REGION_LABEL', 'CA') %}

-- Derive epiweek as YYYYWW using ISO-week (%G%V), which matches FluView's 6-digit format like 202339
WITH base AS (
  SELECT
    DATE(date) AS date,
    LOWER(region_label) AS region,
    CAST(FORMAT_DATE('%G%V', DATE(date)) AS INT64) AS epiweek,
    temp_mean,
    temp_max,
    temp_min
  FROM `{{ env_var('PROJECT', env_var('GOOGLE_CLOUD_PROJECT')) }}.{{ env_var('DBT_RAW','raw') }}.openmeteo_daily_ca`
  WHERE UPPER(region_label) = '{{ region_label | upper }}'
)

SELECT
  epiweek,
  AVG(temp_mean) AS temp_mean_epiwk,
  AVG(temp_max)  AS temp_max_epiwk,
  AVG(temp_min)  AS temp_min_epiwk
FROM base
GROUP BY region, epiweek
ORDER BY epiweek

{{ config(materialized='view', schema=env_var('DBT_SILVER', 'silver')) }}

{% set region_label = env_var('DBT_REGION_LABEL', 'CA') %}

SELECT
  DATE(date)               AS date,
  LOWER(region_label)       AS region,
  AVG(temp_mean)           AS temp_mean,
  AVG(temp_max)            AS temp_max,
  AVG(temp_min)            AS temp_min
FROM `{{ env_var('PROJECT', env_var('GOOGLE_CLOUD_PROJECT')) }}.{{ env_var('DBT_RAW','raw') }}.openmeteo_daily_ca`
WHERE UPPER(region_label) = '{{ region_label | upper }}'
GROUP BY 1,2

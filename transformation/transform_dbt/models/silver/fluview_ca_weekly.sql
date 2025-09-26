{{ config(materialized='view', schema=env_var('DBT_SILVER', 'silver')) }}

{% set flu_regions = env_var('DBT_FLU_REGIONS', 'ca,state:ca').split(',') %}

WITH params AS (
  SELECT region
  FROM UNNEST([
    {% for value in flu_regions %}
      '{{ value.strip().lower() }}'{% if not loop.last %}, {% endif %}
    {% endfor %}
  ]) AS region
)

SELECT
  region,
  epiweek,
  week_start,
  wili,
  ili,
  num_ili,
  num_patients,
  release_date,
  source_url,
  chunk_start,
  chunk_end,
  inserted_at
FROM `{{ env_var('PROJECT', env_var('GOOGLE_CLOUD_PROJECT')) }}.{{ env_var('DBT_RAW','raw') }}.fluview_ca_weekly`
WHERE LOWER(region) IN (SELECT region FROM params)
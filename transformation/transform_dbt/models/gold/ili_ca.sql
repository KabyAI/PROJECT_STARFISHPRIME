{{ config(
    materialized='view',
    schema=env_var('DBT_GOLD', 'gold')
) }}

SELECT
  region,
  epiweek,
  wili,
  ili,
  num_ili,
  num_patients,
  release_date
FROM `{{ env_var('PROJECT') }}.{{ env_var('DBT_SILVER', 'silver') }}.fluview_test`
WHERE region IN ('ca', 'state:ca')

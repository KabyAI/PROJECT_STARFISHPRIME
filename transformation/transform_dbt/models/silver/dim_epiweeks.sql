{{ config(
    materialized='table',
    schema=env_var('DBT_SILVER', 'silver')
) }}

-- Build ~6 years of epiweeks (adjust with DBT_WEEKS_BACK if you want)
WITH params AS (
  SELECT CAST({{ env_var('DBT_WEEKS_BACK', '312') }} AS INT64) AS weeks_back
),
dates AS (
  SELECT d AS dte
  FROM params,
  UNNEST(GENERATE_DATE_ARRAY(
    DATE_SUB(CURRENT_DATE(), INTERVAL weeks_back WEEK),
    CURRENT_DATE(),
    INTERVAL 1 DAY
  )) AS d
)
SELECT
  CAST(FORMAT_DATE('%G%V', dte) AS INT64) AS epiweek,
  MIN(dte) AS start_date,
  MAX(dte) AS end_date
FROM dates
GROUP BY 1
ORDER BY 1

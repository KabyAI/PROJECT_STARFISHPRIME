WITH d AS (
  SELECT
    DATE(date) AS dte,
    region,
    temp_mean, temp_max, temp_min
  FROM {{ ref('weather_ca_daily') }}
),
e AS (
  SELECT * FROM {{ ref('dim_epiweeks') }}
)
SELECT
  'ca' AS region,
  e.epiweek,
  AVG(d.temp_mean) AS temp_mean_epiwk,
  AVG(d.temp_max)  AS temp_max_epiwk,
  AVG(d.temp_min)  AS temp_min_epiwk
FROM d
JOIN e ON e.date = d.dte
GROUP BY region, epiweek

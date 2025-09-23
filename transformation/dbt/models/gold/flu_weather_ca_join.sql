SELECT
  f.region,
  f.epiweek,
  f.wili,
  f.ili,
  w.temp_mean_epiwk,
  w.temp_max_epiwk,
  w.temp_min_epiwk
FROM `project-starfishprime-001.silver.fluview_test` f
LEFT JOIN {{ ref('weather_ca_epiweek') }} w
  ON w.region = 'ca' AND w.epiweek = f.epiweek
WHERE f.region = 'state:ca' OR f.region = 'ca'  -- adjust to your FluView region label

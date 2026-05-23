-- Custom test: fails if any row has NULL temp values
SELECT *
FROM {{ ref('fct_weather_daily') }}
WHERE temperature_max_c IS NULL
   OR temperature_min_c IS NULL
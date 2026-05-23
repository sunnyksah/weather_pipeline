WITH summary AS (
    SELECT * FROM {{ ref('int_daily_summary') }}
)

SELECT
    weather_date,
    EXTRACT(YEAR  FROM weather_date)        AS year,
    EXTRACT(MONTH FROM weather_date)        AS month,
    EXTRACT(DOW   FROM weather_date)        AS day_of_week,   -- 0=Sun
    temperature_max_c,
    temperature_min_c,
    temperature_avg_c,
    temp_range_c,
    precipitation_mm,
    precipitation_category,
    windspeed_max_kmh,
    wind_category,
    -- Rolling 7-day averages (window over the past week)
    ROUND(AVG(temperature_avg_c)   OVER w, 2)   AS temp_avg_7d,
    ROUND(SUM(precipitation_mm)    OVER w, 2)   AS precip_sum_7d,
    loaded_at
FROM summary
WINDOW w AS (ORDER BY weather_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
ORDER BY weather_date DESC
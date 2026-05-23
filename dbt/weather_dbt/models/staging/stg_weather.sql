-- Casts and renames raw columns. Nothing fancy — just clean types.
WITH source AS (
    SELECT * FROM raw.weather_daily
)

SELECT
    date                                    AS weather_date,
    temp_max                                AS temperature_max_c,
    temp_min                                AS temperature_min_c,
    (temp_max + temp_min) / 2.0             AS temperature_avg_c,
    COALESCE(precipitation_mm, 0)           AS precipitation_mm,
    windspeed_max                           AS windspeed_max_kmh,
    loaded_at
FROM source
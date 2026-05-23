
  create view "airflow"."marts_intermediate"."int_daily_summary__dbt_tmp"
    
    
  as (
    WITH staged AS (
    SELECT * FROM "airflow"."marts_staging"."stg_weather"
)

SELECT
    weather_date,
    temperature_max_c,
    temperature_min_c,
    temperature_avg_c,
    temperature_max_c - temperature_min_c   AS temp_range_c,
    precipitation_mm,
    CASE
        WHEN precipitation_mm = 0       THEN 'dry'
        WHEN precipitation_mm < 5       THEN 'light_rain'
        WHEN precipitation_mm < 20      THEN 'moderate_rain'
        ELSE                                 'heavy_rain'
    END                                     AS precipitation_category,
    windspeed_max_kmh,
    CASE
        WHEN windspeed_max_kmh < 20     THEN 'calm'
        WHEN windspeed_max_kmh < 50     THEN 'moderate'
        ELSE                                 'strong'
    END                                     AS wind_category,
    loaded_at
FROM staged
  );
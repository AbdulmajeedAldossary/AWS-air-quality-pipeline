-- Peak Events Analysis for PM10, PM2.5, and NO2 with rounding
SELECT
    'PM10' AS pollutant,
    date, 
    station, 
    ROUND(PM10, 2) AS value,  -- Rounded to 2 decimal places
    TO_CHAR(date, 'Day') AS day_name,
    CASE
        WHEN EXTRACT(MONTH FROM date) BETWEEN 3 AND 5 THEN 'Spring'
        WHEN EXTRACT(MONTH FROM date) BETWEEN 6 AND 8 THEN 'Summer'
        WHEN EXTRACT(MONTH FROM date) BETWEEN 9 AND 11 THEN 'Fall'
        ELSE 'Winter'
    END AS season
FROM (
    SELECT *, RANK() OVER (ORDER BY PM10 DESC) AS rank
    FROM unified_air_quality_imputed
    WHERE PM10 IS NOT NULL AND PM10 = PM10  -- Excludes NULL and NaN
) t
WHERE rank <= 5

UNION ALL

SELECT
    'PM2_5' AS pollutant,
    date, 
    station, 
    ROUND(PM2_5, 2) AS value,  -- Rounded to 2 decimal places
    TO_CHAR(date, 'Day') AS day_name,
    CASE
        WHEN EXTRACT(MONTH FROM date) BETWEEN 3 AND 5 THEN 'Spring'
        WHEN EXTRACT(MONTH FROM date) BETWEEN 6 AND 8 THEN 'Summer'
        WHEN EXTRACT(MONTH FROM date) BETWEEN 9 AND 11 THEN 'Fall'
        ELSE 'Winter'
    END AS season
FROM (
    SELECT *, RANK() OVER (ORDER BY PM2_5 DESC) AS rank
    FROM unified_air_quality_imputed
    WHERE PM2_5 IS NOT NULL AND PM2_5 = PM2_5  -- Excludes NULL and NaN
) t
WHERE rank <= 5

UNION ALL

SELECT
    'NO2' AS pollutant,
    date, 
    station, 
    ROUND(NO2, 2) AS value,  -- Rounded to 2 decimal places
    TO_CHAR(date, 'Day') AS day_name,
    CASE
        WHEN EXTRACT(MONTH FROM date) BETWEEN 3 AND 5 THEN 'Spring'
        WHEN EXTRACT(MONTH FROM date) BETWEEN 6 AND 8 THEN 'Summer'
        WHEN EXTRACT(MONTH FROM date) BETWEEN 9 AND 11 THEN 'Fall'
        ELSE 'Winter'
    END AS season
FROM (
    SELECT *, RANK() OVER (ORDER BY NO2 DESC) AS rank
    FROM unified_air_quality_imputed
    WHERE NO2 IS NOT NULL AND NO2 = NO2  -- Excludes NULL and NaN
) t
WHERE rank <= 5

ORDER BY pollutant, value DESC;
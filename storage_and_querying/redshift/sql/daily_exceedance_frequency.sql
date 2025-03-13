-- Daily Exceedance Frequency
    WITH daily_guidelines AS (
        SELECT 'PM10' AS pollutant, 45 AS daily_guideline UNION ALL
        SELECT 'PM2_5', 15 UNION ALL
        SELECT 'NO2', 25 UNION ALL
        SELECT 'SO2', 40 UNION ALL
        SELECT 'O3', 100 UNION ALL
        SELECT 'CO', 4 UNION ALL
        SELECT 'NH3', 100
    )
    SELECT
        g.pollutant,
        g.daily_guideline || ' µg/m³' AS guideline_value,
        COUNT(CASE 
            WHEN g.pollutant = 'PM10' AND e.PM10 > g.daily_guideline THEN 1
            WHEN g.pollutant = 'PM2_5' AND e.PM2_5 > g.daily_guideline THEN 1
            WHEN g.pollutant = 'NO2' AND e.NO2 > g.daily_guideline THEN 1
            WHEN g.pollutant = 'SO2' AND e.SO2 > g.daily_guideline THEN 1
            WHEN g.pollutant = 'O3' AND e.O3 > g.daily_guideline THEN 1
            WHEN g.pollutant = 'CO' AND e.CO > g.daily_guideline THEN 1
            WHEN g.pollutant = 'NH3' AND e.NH3 > g.daily_guideline THEN 1
        END) AS days_exceeded,
        ROUND(100.0 * COUNT(CASE 
            WHEN g.pollutant = 'PM10' AND e.PM10 > g.daily_guideline THEN 1
            WHEN g.pollutant = 'PM2_5' AND e.PM2_5 > g.daily_guideline THEN 1
            WHEN g.pollutant = 'NO2' AND e.NO2 > g.daily_guideline THEN 1
            WHEN g.pollutant = 'SO2' AND e.SO2 > g.daily_guideline THEN 1
            WHEN g.pollutant = 'O3' AND e.O3 > g.daily_guideline THEN 1
            WHEN g.pollutant = 'CO' AND e.CO > g.daily_guideline THEN 1
            WHEN g.pollutant = 'NH3' AND e.NH3 > g.daily_guideline THEN 1
        END) / COUNT(*), 1) || '%' AS exceedance_percentage,
        ROUND(CASE
            WHEN g.pollutant = 'PM10' THEN MAX(e.PM10)
            WHEN g.pollutant = 'PM2_5' THEN MAX(e.PM2_5)
            WHEN g.pollutant = 'NO2' THEN MAX(e.NO2)
            WHEN g.pollutant = 'SO2' THEN MAX(e.SO2)
            WHEN g.pollutant = 'O3' THEN MAX(e.O3)
            WHEN g.pollutant = 'CO' THEN MAX(e.CO)
            WHEN g.pollutant = 'NH3' THEN MAX(e.NH3)
        END, 1) AS max_value,
        ROUND(CASE
            WHEN g.pollutant = 'PM10' THEN MAX(e.PM10) / g.daily_guideline
            WHEN g.pollutant = 'PM2_5' THEN MAX(e.PM2_5) / g.daily_guideline
            WHEN g.pollutant = 'NO2' THEN MAX(e.NO2) / g.daily_guideline
            WHEN g.pollutant = 'SO2' THEN MAX(e.SO2) / g.daily_guideline
            WHEN g.pollutant = 'O3' THEN MAX(e.O3) / g.daily_guideline
            WHEN g.pollutant = 'CO' THEN MAX(e.CO) / g.daily_guideline
            WHEN g.pollutant = 'NH3' THEN MAX(e.NH3) / g.daily_guideline
        END, 2) AS max_exceedance_ratio
    FROM daily_guidelines g
    CROSS JOIN unified_air_quality_imputed e
    GROUP BY g.pollutant, g.daily_guideline
    ORDER BY days_exceeded DESC;
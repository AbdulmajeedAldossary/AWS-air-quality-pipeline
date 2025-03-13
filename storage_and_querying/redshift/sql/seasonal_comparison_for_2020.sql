 -- Seasonal comparison for 2020
            SELECT
            CASE
                WHEN EXTRACT(MONTH FROM date) BETWEEN 3 AND 5 THEN 'Spring'
                WHEN EXTRACT(MONTH FROM date) BETWEEN 6 AND 8 THEN 'Summer'
                WHEN EXTRACT(MONTH FROM date) BETWEEN 9 AND 11 THEN 'Fall'
                ELSE 'Winter'
            END AS season,
            ROUND(AVG(CO), 2) AS avg_co,
            ROUND(AVG(SO2), 1) AS avg_so2,
            ROUND(AVG(NO2), 1) AS avg_no2,
            ROUND(AVG(O3), 1) AS avg_o3,
            ROUND(AVG(PM10), 1) AS avg_pm10,
            ROUND(AVG(NO), 1) AS avg_no,
            ROUND(AVG(PM2_5), 1) AS avg_pm2_5,
            ROUND(AVG(NH3), 1) AS avg_nh3
        FROM unified_air_quality_imputed
        WHERE EXTRACT(YEAR FROM date) = 2020
        GROUP BY 
            CASE
                WHEN EXTRACT(MONTH FROM date) BETWEEN 3 AND 5 THEN 'Spring'
                WHEN EXTRACT(MONTH FROM date) BETWEEN 6 AND 8 THEN 'Summer'
                WHEN EXTRACT(MONTH FROM date) BETWEEN 9 AND 11 THEN 'Fall'
                ELSE 'Winter'
            END
        ORDER BY 
            CASE 
                CASE
                    WHEN EXTRACT(MONTH FROM date) BETWEEN 3 AND 5 THEN 'Spring'
                    WHEN EXTRACT(MONTH FROM date) BETWEEN 6 AND 8 THEN 'Summer'
                    WHEN EXTRACT(MONTH FROM date) BETWEEN 9 AND 11 THEN 'Fall'
                    ELSE 'Winter'
                END
                WHEN 'Winter' THEN 1
                WHEN 'Spring' THEN 2
                WHEN 'Summer' THEN 3
                WHEN 'Fall' THEN 4
            END;
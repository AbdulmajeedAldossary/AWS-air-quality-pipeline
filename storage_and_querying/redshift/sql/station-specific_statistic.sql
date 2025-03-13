    -- Station-specific statistics
    SELECT
        station,
        ROUND(AVG(CO), 2) AS avg_co,
        ROUND(AVG(SO2), 1) AS avg_so2,
        ROUND(AVG(NO2), 1) AS avg_no2,
        ROUND(AVG(O3), 1) AS avg_o3,
        ROUND(AVG(PM10), 1) AS avg_pm10,
        ROUND(AVG(NO), 1) AS avg_no,
        ROUND(AVG(PM2_5), 1) AS avg_pm2_5,
        ROUND(AVG(NH3), 1) AS avg_nh3
    FROM unified_air_quality_imputed
    GROUP BY station
    ORDER BY avg_pm10 DESC;
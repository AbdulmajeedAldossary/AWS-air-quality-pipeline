     -- Station Comparison with Pollutant Ratios
    SELECT
        station,
        ROUND(AVG(CO), 2) AS avg_co,
        ROUND(AVG(SO2), 1) AS avg_so2,
        ROUND(AVG(NO2), 1) AS avg_no2,
        ROUND(AVG(O3), 1) AS avg_o3,
        ROUND(AVG(PM10), 1) AS avg_pm10,
        ROUND(AVG(NO), 1) AS avg_no,
        ROUND(AVG(PM2_5), 1) AS avg_pm2_5,
        ROUND(AVG(NH3), 1) AS avg_nh3,
        -- Diagnostic Ratios
        ROUND(AVG(PM2_5) / NULLIF(AVG(PM10), 0), 2) AS pm25_pm10_ratio,
        ROUND(AVG(NO2) / NULLIF(AVG(NO), 0), 2) AS no2_no_ratio,
        ROUND(AVG(SO2) / NULLIF(AVG(NO2), 0), 2) AS so2_no2_ratio
    FROM unified_air_quality_imputed
    GROUP BY station
    ORDER BY avg_pm10 DESC;
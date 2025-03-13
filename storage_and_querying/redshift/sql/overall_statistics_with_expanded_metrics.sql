  --- Overall statistics with expanded metrics
    SELECT
        COUNT(*) AS record_count,
        ROUND(AVG(CO), 2) AS avg_co,
        ROUND(STDDEV(CO), 2) AS stddev_co,
        ROUND(MIN(CO), 2) AS min_co,
        ROUND(MAX(CO), 2) AS max_co,
        ROUND(AVG(SO2), 1) AS avg_so2,
        ROUND(AVG(NO2), 1) AS avg_no2,
        ROUND(AVG(O3), 1) AS avg_o3,
        ROUND(AVG(PM10), 1) AS avg_pm10,
        ROUND(AVG(NO), 1) AS avg_no,
        ROUND(AVG(PM2_5), 1) AS avg_pm2_5,
        ROUND(AVG(NH3), 1) AS avg_nh3
    FROM unified_air_quality_imputed;
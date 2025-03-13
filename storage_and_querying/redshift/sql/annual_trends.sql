SELECT
            EXTRACT(YEAR FROM date) AS year,
            ROUND(AVG(CO), 2) AS avg_co,
            ROUND(AVG(SO2), 1) AS avg_so2,
            ROUND(AVG(NO2), 1) AS avg_no2,
            ROUND(AVG(O3), 1) AS avg_o3,
            ROUND(AVG(PM10), 1) AS avg_pm10,
            ROUND(AVG(NO), 1) AS avg_no,
            ROUND(AVG(PM2_5), 1) AS avg_pm2_5,
            ROUND(AVG(NH3), 1) AS avg_nh3,
            -- Year-over-year changes for PM10
            ROUND(100 * (AVG(PM10) - LAG(AVG(PM10), 1) OVER (ORDER BY EXTRACT(YEAR FROM date))) /
                NULLIF(LAG(AVG(PM10), 1) OVER (ORDER BY EXTRACT(YEAR FROM date)), 0), 2) AS pm10_yoy_pct_change
        FROM unified_air_quality_imputed
        GROUP BY EXTRACT(YEAR FROM date)
        ORDER BY year;
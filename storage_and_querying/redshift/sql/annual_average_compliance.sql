 -- Annual Average Compliance
    SELECT
        EXTRACT(YEAR FROM date) AS year,
        -- PM10 Annual (WHO 2021: 15 µg/m³)
        ROUND(AVG(PM10), 1) AS annual_avg_pm10,
        CASE WHEN AVG(PM10) > 15 THEN 'Exceeds' ELSE 'Within' END AS pm10_who_status,
        ROUND(AVG(PM10) / 15, 2) AS pm10_who_ratio,
        -- PM2.5 Annual (WHO 2021: 5 µg/m³)
        ROUND(AVG(PM2_5), 1) AS annual_avg_pm2_5,
        CASE WHEN AVG(PM2_5) > 5 THEN 'Exceeds' ELSE 'Within' END AS pm2_5_who_status,
        ROUND(AVG(PM2_5) / 5, 2) AS pm2_5_who_ratio,
        -- NO2 Annual (WHO 2021: 10 µg/m³)
        ROUND(AVG(NO2), 1) AS annual_avg_no2,
        CASE WHEN AVG(NO2) > 10 THEN 'Exceeds' ELSE 'Within' END AS no2_who_status,
        ROUND(AVG(NO2) / 10, 2) AS no2_who_ratio,
        -- SO2 24-hour (WHO 2021: 40 µg/m³)
        ROUND(AVG(SO2), 1) AS annual_avg_so2,
        CASE WHEN AVG(SO2) > 40 THEN 'Exceeds' ELSE 'Within' END AS so2_who_status
    FROM unified_air_quality_imputed
    GROUP BY EXTRACT(YEAR FROM date)
    ORDER BY year;
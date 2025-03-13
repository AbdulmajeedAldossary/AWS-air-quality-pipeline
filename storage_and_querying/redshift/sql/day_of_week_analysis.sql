 -- Day of Week Analysis
    SELECT
        EXTRACT(DOW FROM date) AS day_of_week,
        TO_CHAR(date, 'Day') AS day_name,
        ROUND(AVG(CO), 2) AS avg_co,
        ROUND(AVG(NO2), 1) AS avg_no2,
        ROUND(AVG(PM10), 1) AS avg_pm10
    FROM unified_air_quality_imputed
    GROUP BY EXTRACT(DOW FROM date), TO_CHAR(date, 'Day')
    ORDER BY day_of_week;
-- Step 1B: Handle consecutive missing values with forward fill
    CREATE TABLE unified_air_quality_imputed AS
    SELECT
        date,
        COALESCE(CO, LEAD(CO) OVER (PARTITION BY station ORDER BY date)) AS CO,
        COALESCE(SO2, LEAD(SO2) OVER (PARTITION BY station ORDER BY date)) AS SO2,
        COALESCE(NO2, LEAD(NO2) OVER (PARTITION BY station ORDER BY date)) AS NO2,
        COALESCE(O3, LEAD(O3) OVER (PARTITION BY station ORDER BY date)) AS O3,
        COALESCE(PM10, LEAD(PM10) OVER (PARTITION BY station ORDER BY date)) AS PM10,
        COALESCE(NO, LEAD(NO) OVER (PARTITION BY station ORDER BY date)) AS NO,
        COALESCE(PM2_5, LEAD(PM2_5) OVER (PARTITION BY station ORDER BY date)) AS PM2_5,
        COALESCE(NH3, LEAD(NH3) OVER (PARTITION BY station ORDER BY date)) AS NH3,
        station,
        longitude,
        latitude
    FROM unified_air_quality_imputed_temp;
-- Step 1A: Initial imputation using backward fill
    CREATE TABLE unified_air_quality_imputed_temp AS
    SELECT
        date,
        COALESCE(CO, LAG(CO) OVER (PARTITION BY station ORDER BY date)) AS CO,
        COALESCE(SO2, LAG(SO2) OVER (PARTITION BY station ORDER BY date)) AS SO2,
        COALESCE(NO2, LAG(NO2) OVER (PARTITION BY station ORDER BY date)) AS NO2,
        COALESCE(O3, LAG(O3) OVER (PARTITION BY station ORDER BY date)) AS O3,
        COALESCE(PM10, LAG(PM10) OVER (PARTITION BY station ORDER BY date)) AS PM10,
        COALESCE(NO, LAG(NO) OVER (PARTITION BY station ORDER BY date)) AS NO,
        COALESCE(PM2_5, LAG(PM2_5) OVER (PARTITION BY station ORDER BY date)) AS PM2_5,
        COALESCE(NH3, LAG(NH3) OVER (PARTITION BY station ORDER BY date)) AS NH3,
        station,
        longitude,
        latitude
    FROM unified_air_quality;

INSERT INTO unified_air_quality (date, CO, SO2, NO2, O3, PM10)
     SELECT 
         TO_DATE(date, 'YYYY-MM-DD') AS date,
         CO,
         SO2,
         NO2,
         O3,
         PM10
     FROM air_quality_data
     WHERE TO_DATE(date, 'YYYY-MM-DD') >= '2019-08-01'
       AND TO_DATE(date, 'YYYY-MM-DD') <= '2020-11-26'; 
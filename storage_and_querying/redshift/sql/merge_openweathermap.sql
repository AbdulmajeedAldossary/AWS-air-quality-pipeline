INSERT INTO unified_air_quality (date, CO, SO2, NO2, O3, PM10, NO, PM2_5, NH3, longitude, latitude)
     SELECT 
         DATE_TRUNC('day', date) AS date,
         AVG(co) AS CO,
         AVG(so2) AS SO2,
         AVG(no2) AS NO2,
         AVG(o3) AS O3,
         AVG(pm10) AS PM10,
         AVG(no) AS NO,
         AVG(pm2_5) AS PM2_5,
         AVG(nh3) AS NH3,
         longitude,
         latitude
     FROM openweathermap_air_quality_data
     WHERE date >= '2020-11-27'
     GROUP BY DATE_TRUNC('day', date), longitude, latitude;
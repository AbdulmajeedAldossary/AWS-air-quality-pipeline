WITH pollutants AS (
    SELECT 'CO' AS pollutant UNION ALL
    SELECT 'NO' UNION ALL
    SELECT 'NO2' UNION ALL
    SELECT 'SO2' UNION ALL
    SELECT 'O3' UNION ALL
    SELECT 'PM10' UNION ALL
    SELECT 'PM2_5' UNION ALL
    SELECT 'NH3'
),
pollutant_pairs AS (
    SELECT 
        p1.pollutant AS pollutant_1,
        p2.pollutant AS pollutant_2,
        p1.pollutant || '-' || p2.pollutant AS pollutant_pair
    FROM pollutants p1
    CROSS JOIN pollutants p2
),
stats AS (
    SELECT
        -- Count valid pairs for each combination
        COUNT(CASE WHEN CO IS NOT NULL AND NO2 IS NOT NULL AND CO = CO AND NO2 = NO2 THEN 1 END) AS n_co_no2,
        COUNT(CASE WHEN CO IS NOT NULL AND NO IS NOT NULL AND CO = CO AND NO = NO THEN 1 END) AS n_co_no,
        COUNT(CASE WHEN CO IS NOT NULL AND SO2 IS NOT NULL AND CO = CO AND SO2 = SO2 THEN 1 END) AS n_co_so2,
        COUNT(CASE WHEN CO IS NOT NULL AND O3 IS NOT NULL AND CO = CO AND O3 = O3 THEN 1 END) AS n_co_o3,
        COUNT(CASE WHEN CO IS NOT NULL AND PM10 IS NOT NULL AND CO = CO AND PM10 = PM10 THEN 1 END) AS n_co_pm10,
        COUNT(CASE WHEN CO IS NOT NULL AND PM2_5 IS NOT NULL AND CO = CO AND PM2_5 = PM2_5 THEN 1 END) AS n_co_pm2_5,
        COUNT(CASE WHEN CO IS NOT NULL AND NH3 IS NOT NULL AND CO = CO AND NH3 = NH3 THEN 1 END) AS n_co_nh3,
        COUNT(CASE WHEN NO IS NOT NULL AND NO2 IS NOT NULL AND NO = NO AND NO2 = NO2 THEN 1 END) AS n_no_no2,
        COUNT(CASE WHEN NO2 IS NOT NULL AND O3 IS NOT NULL AND NO2 = NO2 AND O3 = O3 THEN 1 END) AS n_no2_o3,
        COUNT(CASE WHEN PM10 IS NOT NULL AND PM2_5 IS NOT NULL AND PM10 = PM10 AND PM2_5 = PM2_5 THEN 1 END) AS n_pm10_pm2_5,
        COUNT(CASE WHEN SO2 IS NOT NULL AND NO2 IS NOT NULL AND SO2 = SO2 AND NO2 = NO2 THEN 1 END) AS n_so2_no2,
        COUNT(CASE WHEN SO2 IS NOT NULL AND NH3 IS NOT NULL AND SO2 = SO2 AND NH3 = NH3 THEN 1 END) AS n_so2_nh3,
        COUNT(CASE WHEN NH3 IS NOT NULL AND NO2 IS NOT NULL AND NH3 = NH3 AND NO2 = NO2 THEN 1 END) AS n_nh3_no2,
        
        -- Sums for each pollutant
        SUM(CASE WHEN CO IS NOT NULL AND CO = CO THEN CO ELSE 0 END) AS sum_co,
        SUM(CASE WHEN NO2 IS NOT NULL AND NO2 = NO2 THEN NO2 ELSE 0 END) AS sum_no2,
        SUM(CASE WHEN NO IS NOT NULL AND NO = NO THEN NO ELSE 0 END) AS sum_no,
        SUM(CASE WHEN SO2 IS NOT NULL AND SO2 = SO2 THEN SO2 ELSE 0 END) AS sum_so2,
        SUM(CASE WHEN O3 IS NOT NULL AND O3 = O3 THEN O3 ELSE 0 END) AS sum_o3,
        SUM(CASE WHEN PM10 IS NOT NULL AND PM10 = PM10 THEN PM10 ELSE 0 END) AS sum_pm10,
        SUM(CASE WHEN PM2_5 IS NOT NULL AND PM2_5 = PM2_5 THEN PM2_5 ELSE 0 END) AS sum_pm2_5,
        SUM(CASE WHEN NH3 IS NOT NULL AND NH3 = NH3 THEN NH3 ELSE 0 END) AS sum_nh3,
        
        -- Sum of squares
        SUM(CASE WHEN CO IS NOT NULL AND CO = CO THEN CO*CO ELSE 0 END) AS sum_sq_co,
        SUM(CASE WHEN NO2 IS NOT NULL AND NO2 = NO2 THEN NO2*NO2 ELSE 0 END) AS sum_sq_no2,
        SUM(CASE WHEN NO IS NOT NULL AND NO = NO THEN NO*NO ELSE 0 END) AS sum_sq_no,
        SUM(CASE WHEN SO2 IS NOT NULL AND SO2 = SO2 THEN SO2*SO2 ELSE 0 END) AS sum_sq_so2,
        SUM(CASE WHEN O3 IS NOT NULL AND O3 = O3 THEN O3*O3 ELSE 0 END) AS sum_sq_o3,
        SUM(CASE WHEN PM10 IS NOT NULL AND PM10 = PM10 THEN PM10*PM10 ELSE 0 END) AS sum_sq_pm10,
        SUM(CASE WHEN PM2_5 IS NOT NULL AND PM2_5 = PM2_5 THEN PM2_5*PM2_5 ELSE 0 END) AS sum_sq_pm2_5,
        SUM(CASE WHEN NH3 IS NOT NULL AND NH3 = NH3 THEN NH3*NH3 ELSE 0 END) AS sum_sq_nh3,
        
        -- Sum of products for each pair
        SUM(CASE WHEN CO IS NOT NULL AND NO2 IS NOT NULL AND CO = CO AND NO2 = NO2 THEN CO*NO2 ELSE 0 END) AS sum_co_no2,
        SUM(CASE WHEN CO IS NOT NULL AND NO IS NOT NULL AND CO = CO AND NO = NO THEN CO*NO ELSE 0 END) AS sum_co_no,
        SUM(CASE WHEN CO IS NOT NULL AND SO2 IS NOT NULL AND CO = CO AND SO2 = SO2 THEN CO*SO2 ELSE 0 END) AS sum_co_so2,
        SUM(CASE WHEN CO IS NOT NULL AND O3 IS NOT NULL AND CO = CO AND O3 = O3 THEN CO*O3 ELSE 0 END) AS sum_co_o3,
        SUM(CASE WHEN CO IS NOT NULL AND PM10 IS NOT NULL AND CO = CO AND PM10 = PM10 THEN CO*PM10 ELSE 0 END) AS sum_co_pm10,
        SUM(CASE WHEN CO IS NOT NULL AND PM2_5 IS NOT NULL AND CO = CO AND PM2_5 = PM2_5 THEN CO*PM2_5 ELSE 0 END) AS sum_co_pm2_5,
        SUM(CASE WHEN CO IS NOT NULL AND NH3 IS NOT NULL AND CO = CO AND NH3 = NH3 THEN CO*NH3 ELSE 0 END) AS sum_co_nh3,
        SUM(CASE WHEN NO IS NOT NULL AND NO2 IS NOT NULL AND NO = NO AND NO2 = NO2 THEN NO*NO2 ELSE 0 END) AS sum_no_no2,
        SUM(CASE WHEN NO2 IS NOT NULL AND O3 IS NOT NULL AND NO2 = NO2 AND O3 = O3 THEN NO2*O3 ELSE 0 END) AS sum_no2_o3,
        SUM(CASE WHEN PM10 IS NOT NULL AND PM2_5 IS NOT NULL AND PM10 = PM10 AND PM2_5 = PM2_5 THEN PM10*PM2_5 ELSE 0 END) AS sum_pm10_pm2_5,
        SUM(CASE WHEN SO2 IS NOT NULL AND NO2 IS NOT NULL AND SO2 = SO2 AND NO2 = NO2 THEN SO2*NO2 ELSE 0 END) AS sum_so2_no2,
        SUM(CASE WHEN SO2 IS NOT NULL AND NH3 IS NOT NULL AND SO2 = SO2 AND NH3 = NH3 THEN SO2*NH3 ELSE 0 END) AS sum_so2_nh3,
        SUM(CASE WHEN NH3 IS NOT NULL AND NO2 IS NOT NULL AND NH3 = NH3 AND NO2 = NO2 THEN NH3*NO2 ELSE 0 END) AS sum_nh3_no2
    FROM unified_air_quality_imputed
),
correlations AS (
    -- CO-NO2
    SELECT 'CO-NO2' AS pollutant_pair,
        CASE 
            WHEN (n_co_no2 * sum_sq_co - sum_co * sum_co) <= 0 OR (n_co_no2 * sum_sq_no2 - sum_no2 * sum_no2) <= 0 
            THEN NULL
            ELSE
                GREATEST(-1, LEAST(1, 
                    ROUND(
                        (n_co_no2::numeric * sum_co_no2 - sum_co * sum_no2) / 
                        (SQRT(GREATEST(0, n_co_no2::numeric * sum_sq_co - sum_co * sum_co)) * 
                         SQRT(GREATEST(0, n_co_no2::numeric * sum_sq_no2 - sum_no2 * sum_no2))),
                        2
                    )
                ))
        END AS correlation_coef
    FROM stats
    WHERE n_co_no2 > 0
    UNION ALL
    SELECT 'CO-NO',
        CASE 
            WHEN (n_co_no * sum_sq_co - sum_co * sum_co) <= 0 OR (n_co_no * sum_sq_no - sum_no * sum_no) <= 0 
            THEN NULL
            ELSE
                GREATEST(-1, LEAST(1,
                    ROUND(
                        (n_co_no::numeric * sum_co_no - sum_co * sum_no) / 
                        (SQRT(GREATEST(0, n_co_no::numeric * sum_sq_co - sum_co * sum_co)) * 
                         SQRT(GREATEST(0, n_co_no::numeric * sum_sq_no - sum_no * sum_no))),
                        2
                    )
                ))
        END
    FROM stats
    WHERE n_co_no > 0
    UNION ALL
    SELECT 'CO-SO2',
        CASE 
            WHEN (n_co_so2 * sum_sq_co - sum_co * sum_co) <= 0 OR (n_co_so2 * sum_sq_so2 - sum_so2 * sum_so2) <= 0 
            THEN NULL
            ELSE
                GREATEST(-1, LEAST(1,
                    ROUND(
                        (n_co_so2::numeric * sum_co_so2 - sum_co * sum_so2) / 
                        (SQRT(GREATEST(0, n_co_so2::numeric * sum_sq_co - sum_co * sum_co)) * 
                         SQRT(GREATEST(0, n_co_so2::numeric * sum_sq_so2 - sum_so2 * sum_so2))),
                        2
                    )
                ))
        END
    FROM stats
    WHERE n_co_so2 > 0
    UNION ALL
    SELECT 'CO-O3',
        CASE 
            WHEN (n_co_o3 * sum_sq_co - sum_co * sum_co) <= 0 OR (n_co_o3 * sum_sq_o3 - sum_o3 * sum_o3) <= 0 
            THEN NULL
            ELSE
                GREATEST(-1, LEAST(1,
                    ROUND(
                        (n_co_o3::numeric * sum_co_o3 - sum_co * sum_o3) / 
                        (SQRT(GREATEST(0, n_co_o3::numeric * sum_sq_co - sum_co * sum_co)) * 
                         SQRT(GREATEST(0, n_co_o3::numeric * sum_sq_o3 - sum_o3 * sum_o3))),
                        2
                    )
                ))
        END
    FROM stats
    WHERE n_co_o3 > 0
    UNION ALL
    SELECT 'CO-PM10',
        CASE 
            WHEN (n_co_pm10 * sum_sq_co - sum_co * sum_co) <= 0 OR (n_co_pm10 * sum_sq_pm10 - sum_pm10 * sum_pm10) <= 0 
            THEN NULL
            ELSE
                GREATEST(-1, LEAST(1,
                    ROUND(
                        (n_co_pm10::numeric * sum_co_pm10 - sum_co * sum_pm10) / 
                        (SQRT(GREATEST(0, n_co_pm10::numeric * sum_sq_co - sum_co * sum_co)) * 
                         SQRT(GREATEST(0, n_co_pm10::numeric * sum_sq_pm10 - sum_pm10 * sum_pm10))),
                        2
                    )
                ))
        END
    FROM stats
    WHERE n_co_pm10 > 0
    UNION ALL
    SELECT 'CO-PM2_5',
        CASE 
            WHEN (n_co_pm2_5 * sum_sq_co - sum_co * sum_co) <= 0 OR (n_co_pm2_5 * sum_sq_pm2_5 - sum_pm2_5 * sum_pm2_5) <= 0 
            THEN NULL
            ELSE
                GREATEST(-1, LEAST(1,
                    ROUND(
                        (n_co_pm2_5::numeric * sum_co_pm2_5 - sum_co * sum_pm2_5) / 
                        (SQRT(GREATEST(0, n_co_pm2_5::numeric * sum_sq_co - sum_co * sum_co)) * 
                         SQRT(GREATEST(0, n_co_pm2_5::numeric * sum_sq_pm2_5 - sum_pm2_5 * sum_pm2_5))),
                        2
                    )
                ))
        END
    FROM stats
    WHERE n_co_pm2_5 > 0
    UNION ALL
    SELECT 'CO-NH3',
        CASE 
            WHEN (n_co_nh3 * sum_sq_co - sum_co * sum_co) <= 0 OR (n_co_nh3 * sum_sq_nh3 - sum_nh3 * sum_nh3) <= 0 
            THEN NULL
            ELSE
                GREATEST(-1, LEAST(1,
                    ROUND(
                        (n_co_nh3::numeric * sum_co_nh3 - sum_co * sum_nh3) / 
                        (SQRT(GREATEST(0, n_co_nh3::numeric * sum_sq_co - sum_co * sum_co)) * 
                         SQRT(GREATEST(0, n_co_nh3::numeric * sum_sq_nh3 - sum_nh3 * sum_nh3))),
                        2
                    )
                ))
        END
    FROM stats
    WHERE n_co_nh3 > 0
    UNION ALL
    SELECT 'NO-NO2',
        CASE 
            WHEN (n_no_no2 * sum_sq_no - sum_no * sum_no) <= 0 OR (n_no_no2 * sum_sq_no2 - sum_no2 * sum_no2) <= 0 
            THEN NULL
            ELSE
                GREATEST(-1, LEAST(1,
                    ROUND(
                        (n_no_no2::numeric * sum_no_no2 - sum_no * sum_no2) / 
                        (SQRT(GREATEST(0, n_no_no2::numeric * sum_sq_no - sum_no * sum_no)) * 
                         SQRT(GREATEST(0, n_no_no2::numeric * sum_sq_no2 - sum_no2 * sum_no2))),
                        2
                    )
                ))
        END
    FROM stats
    WHERE n_no_no2 > 0
    UNION ALL
    SELECT 'NO2-O3',
        CASE 
            WHEN (n_no2_o3 * sum_sq_no2 - sum_no2 * sum_no2) <= 0 OR (n_no2_o3 * sum_sq_o3 - sum_o3 * sum_o3) <= 0 
            THEN NULL
            ELSE
                GREATEST(-1, LEAST(1,
                    ROUND(
                        (n_no2_o3::numeric * sum_no2_o3 - sum_no2 * sum_o3) / 
                        (SQRT(GREATEST(0, n_no2_o3::numeric * sum_sq_no2 - sum_no2 * sum_no2)) * 
                         SQRT(GREATEST(0, n_no2_o3::numeric * sum_sq_o3 - sum_o3 * sum_o3))),
                        2
                    )
                ))
        END
    FROM stats
    WHERE n_no2_o3 > 0
    UNION ALL
    SELECT 'PM10-PM2_5',
        CASE 
            WHEN (n_pm10_pm2_5 * sum_sq_pm10 - sum_pm10 * sum_pm10) <= 0 OR (n_pm10_pm2_5 * sum_sq_pm2_5 - sum_pm2_5 * sum_pm2_5) <= 0 
            THEN NULL
            ELSE
                GREATEST(-1, LEAST(1,
                    ROUND(
                        (n_pm10_pm2_5::numeric * sum_pm10_pm2_5 - sum_pm10 * sum_pm2_5) / 
                        (SQRT(GREATEST(0, n_pm10_pm2_5::numeric * sum_sq_pm10 - sum_pm10 * sum_pm10)) * 
                         SQRT(GREATEST(0, n_pm10_pm2_5::numeric * sum_sq_pm2_5 - sum_pm2_5 * sum_pm2_5))),
                        2
                    )
                ))
        END
    FROM stats
    WHERE n_pm10_pm2_5 > 0
    UNION ALL
    SELECT 'SO2-NO2',
        CASE 
            WHEN (n_so2_no2 * sum_sq_so2 - sum_so2 * sum_so2) <= 0 OR (n_so2_no2 * sum_sq_no2 - sum_no2 * sum_no2) <= 0 
            THEN NULL
            ELSE
                GREATEST(-1, LEAST(1,
                    ROUND(
                        (n_so2_no2::numeric * sum_so2_no2 - sum_so2 * sum_no2) / 
                        (SQRT(GREATEST(0, n_so2_no2::numeric * sum_sq_so2 - sum_so2 * sum_so2)) * 
                         SQRT(GREATEST(0, n_so2_no2::numeric * sum_sq_no2 - sum_no2 * sum_no2))),
                        2
                    )
                ))
        END
    FROM stats
    WHERE n_so2_no2 > 0
    UNION ALL
    SELECT 'SO2-NH3',
        CASE 
            WHEN (n_so2_nh3 * sum_sq_so2 - sum_so2 * sum_so2) <= 0 OR (n_so2_nh3 * sum_sq_nh3 - sum_nh3 * sum_nh3) <= 0 
            THEN NULL
            ELSE
                GREATEST(-1, LEAST(1,
                    ROUND(
                        (n_so2_nh3::numeric * sum_so2_nh3 - sum_so2 * sum_nh3) / 
                        (SQRT(GREATEST(0, n_so2_nh3::numeric * sum_sq_so2 - sum_so2 * sum_so2)) * 
                         SQRT(GREATEST(0, n_so2_nh3::numeric * sum_sq_nh3 - sum_nh3 * sum_nh3))),
                        2
                    )
                ))
        END
    FROM stats
    WHERE n_so2_nh3 > 0
    UNION ALL
    SELECT 'NH3-NO2',
        CASE 
            WHEN (n_nh3_no2 * sum_sq_nh3 - sum_nh3 * sum_nh3) <= 0 OR (n_nh3_no2 * sum_sq_no2 - sum_no2 * sum_no2) <= 0 
            THEN NULL
            ELSE
                GREATEST(-1, LEAST(1,
                    ROUND(
                        (n_nh3_no2::numeric * sum_nh3_no2 - sum_nh3 * sum_no2) / 
                        (SQRT(GREATEST(0, n_nh3_no2::numeric * sum_sq_nh3 - sum_nh3 * sum_nh3)) * 
                         SQRT(GREATEST(0, n_nh3_no2::numeric * sum_sq_no2 - sum_no2 * sum_no2))),
                        2
                    )
                ))
        END
    FROM stats
    WHERE n_nh3_no2 > 0
    UNION ALL
    SELECT 'CO-CO', 1.0 UNION ALL
    SELECT 'NO-NO', 1.0 UNION ALL
    SELECT 'NO2-NO2', 1.0 UNION ALL
    SELECT 'SO2-SO2', 1.0 UNION ALL
    SELECT 'O3-O3', 1.0 UNION ALL
    SELECT 'PM10-PM10', 1.0 UNION ALL
    SELECT 'PM2_5-PM2_5', 1.0 UNION ALL
    SELECT 'NH3-NH3', 1.0
),
symmetric_correlations AS (
    SELECT pollutant_pair, correlation_coef
    FROM correlations
    WHERE correlation_coef IS NOT NULL
    UNION ALL
    SELECT 
        SPLIT_PART(pollutant_pair, '-', 2) || '-' || SPLIT_PART(pollutant_pair, '-', 1) AS pollutant_pair,
        correlation_coef
    FROM correlations
    WHERE correlation_coef IS NOT NULL
    AND SPLIT_PART(pollutant_pair, '-', 1) != SPLIT_PART(pollutant_pair, '-', 2)
)
SELECT pollutant_pair, correlation_coef
FROM symmetric_correlations
ORDER BY ABS(correlation_coef) DESC;
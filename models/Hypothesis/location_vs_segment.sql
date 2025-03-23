-- 1 -> smb
-- 2 -> commerical
-- 3 -> ent
-- 4 -> midmarket
-- 5 -> non smb


WITH Vini as (
    SELECT
        id,
        -- Handle segment_type
            CASE 
                WHEN segment_smb = 1 THEN 1
                WHEN segment_commercial = 1 THEN 2
                WHEN segment_enterprise = 1 THEN 3 
                WHEN segment_midmarket = 1 THEN 4
                WHEN segment_non_smb = 1 THEN 5
            END as segment_type,

        -- Handle customer_location
            CASE 
                WHEN region_emea = 1 THEN 1 
                WHEN region_apac = 1 THEN 2 
                WHEN region_latam = 1 THEN 3 
                WHEN region_amer = 1 THEN 4
            END as customer_location
    FROM {{source('vini_public', 'customer')}}
)

SELECT
    customer_location,
    segment_type,
    COUNT(DISTINCT id) as no_of_type
FROM Vini
GROUP BY customer_location, segment_type
ORDER BY customer_location, segment_type
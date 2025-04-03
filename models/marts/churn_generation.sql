{{ config(materialized='table') }}
WITH exp AS (
    SELECT * 
    FROM {{source('vini_public', 'expansion_outcome')}}
),

temp_exp AS (
    SELECT 
        id, 
        AVG(future_arr) AS future_arr,
        AVG(current_arr) AS curr_arr
    FROM exp
    GROUP BY id
),

cust AS (
    SELECT * 
    FROM {{source('vini_public', 'customer')}}
),

feed AS (
    SELECT 
        id, 
        AVG(csat_score) AS csat_score,
        MAX(months_since_high_sev_outage) AS last_outage
    FROM {{source('vini_public', 'customer_feedback')}}
    GROUP BY id
),

util AS (
    SELECT 
        id, 
        AVG(seat_utilization) AS seat_util
    FROM {{source('vini_public', 'utilization')}}
    GROUP BY id
),

final AS (
    SELECT 
        t1.id,
        t1.future_arr,
        t1.curr_arr,
        COALESCE(t2.csat_score, 0) AS csat_score,  -- Handle NULLs
        COALESCE(t2.last_outage, 999) AS last_outage,  -- Handle NULLs (Assume large value means "No recent outage")
        COALESCE(t3.seat_util, 1.0) AS seat_util  -- Handle NULLs (Default to full utilization)
    FROM temp_exp AS t1 
    LEFT JOIN feed AS t2 ON t1.id = t2.id
    LEFT JOIN util AS t3 ON t1.id = t3.id
),

final_last as (
        SELECT
            *,
        CASE
            WHEN csat_score < 50 THEN 1
            WHEN last_outage < 5 THEN 1
            WHEN future_arr < curr_arr THEN 1
            WHEN seat_util < 0.2 THEN 1
            ELSE 0
        END AS churned
    FROM final
    ORDER BY id
)


SELECT 
    t1.*,
    t2.future_arr,
    t2.curr_arr,
    t2.csat_score,
    t2.last_outage,
    t2.seat_util,
    t2.churned
FROM cust as t1
JOIN final_last as t2 ON t1.id = t2.id


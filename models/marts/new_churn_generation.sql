{{ config(materialized='table') }}

WITH cust AS (
    SELECT 
        *,
        TO_DATE(REPLACE(score_date, '/', '-'), 'DD-MM-YYYY') AS new_score_date
    FROM {{ source('vini_public', 'customer') }}
),

exp AS (
    SELECT
        id, 
        TO_DATE(REPLACE(score_date, '/', '-'), 'DD-MM-YYYY') AS new_score_date,
        future_arr,
        current_arr 
    FROM {{source('vini_public', 'expansion_outcome')}}
),

feed AS (
    SELECT
        id, 
        TO_DATE(REPLACE(score_date, '/', '-'), 'DD-MM-YYYY') AS new_score_date,
        csat_score,
        months_since_high_sev_outage AS last_outage
    FROM {{source('vini_public', 'customer_feedback')}}
),

util AS (
    SELECT 
        id, 
        TO_DATE(REPLACE(score_date, '/', '-'), 'DD-MM-YYYY') AS new_score_date,
        seat_utilization
    FROM {{source('vini_public', 'utilization')}}

),

Gojo AS (
    SELECT 
        t1.*,
        t2.seat_utilization,
        t3.csat_score,
        t3.last_outage,
        COALESCE(t4.future_arr, 0) AS future_arr,
        t4.current_arr
    FROM cust as t1
    JOIN util as t2 ON t1.id = t2.id AND t1.new_score_date = t2.new_score_date
    JOIN feed as t3 ON t1.id = t3.id AND t1.new_score_date = t3.new_score_date
    JOIN exp as t4 ON t1.id = t4.id AND t1.new_score_date = t4.new_score_date
),

last_value_cte as (
    SELECT
        *,
        LAST_VALUE(new_score_date) OVER (
            PARTITION BY id 
            ORDER BY new_score_date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS last_score_date
    FROM Gojo
),

final AS (
    SELECT
        *,
        CASE
            WHEN LEAD(new_score_date) OVER (PARTITION BY id ORDER BY new_score_date) > new_score_date + INTERVAL '7 days' 
                THEN 1
            WHEN last_score_date = new_score_date 
                 AND EXTRACT(MONTH FROM last_score_date) IN (1, 2) 
                THEN 1
            WHEN last_score_date = new_score_date 
                 AND EXTRACT(MONTH FROM last_score_date) = 3 
                 AND EXTRACT(DAY FROM last_score_date) <> 31 
                THEN 1
            WHEN future_arr = 0 THEN 1
            ELSE 0
        END AS churned
    FROM last_value_cte
)


SELECT *
FROM final


WITH Vini as (
    SELECT 
        id,
        CASE
            WHEN LAG(ent_plus) OVER(PARTITION BY id ORDER BY score_date) < ent_plus THEN 1
            WHEN LAG(pro_plus) OVER(PARTITION BY id ORDER BY score_date) < pro_plus THEN 1
            WHEN LAG(team_plus) OVER(PARTITION BY id ORDER BY score_date) < team_plus THEN 1 ELSE 0
        END as upsell_flag
    FROM {{source('vini_staging', 'stg_product')}}
),

Vars as (
    SELECT
        id
    FROM Vini
    WHERE upsell_flag = 1
    GROUP BY id
),

cust as (
    SELECT
        *
    FROM {{source('vini_public', 'customer')}}
),

util as (
    SELECT
        *
    FROM {{source('vini_public', 'utilization')}}
),

cust_feed as (
    SELECT
        *
    FROM {{source('vini_public', 'customer_feedback')}}
),

exp_outcome as (
    SELECT
        *
    FROM {{source('vini_public', 'expansion_outcome')}}
)


SELECT f.*
FROM Vini as f
JOIN cust as s ON f.id = s.id
JOIN cust_feed as t ON f.id = t.id
JOIN exp_outcome as a ON f.id = a.id
JOIN util as ss ON f.id = ss.id




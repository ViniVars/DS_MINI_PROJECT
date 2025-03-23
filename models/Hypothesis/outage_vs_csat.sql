WITH Vini as (
    SELECT
        id,
        MAX(months_since_high_sev_outage) as last_outage,
        AVG(csat_score) as csat_score
    FROM {{source('vini_public', 'customer_feedback')}}
    GROUP BY id
),

Vars as(
    SELECT 
        COALESCE(last_outage, 100) as last_outage,
        AVG(COALESCE(csat_score, 0)) as avg_csat_score
    FROM Vini
    GROUP BY COALESCE(last_outage, 100)
)

SELECT *
FROM Vars
WHERE last_outage <> 100
ORDER BY last_outage



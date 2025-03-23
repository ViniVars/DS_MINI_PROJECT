WITH Vini as(
    SELECT *
    FROM {{source('vini_public', 'product')}}
)

SELECT 
    id,
    TO_DATE(score_date, 'DD-MM-YYYY') as score_date,
    discount_arr_usd,
    team_plus,
    pro_plus,
    ent_plus,
    discount_arr_usd_percentage,
    product_counts_percentage,
    total_pool_max_agents
FROM Vini
ORDER BY score_date
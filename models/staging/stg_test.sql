WITH prod as (
    SELECT
        id,
        score_date,
        team_plus,
        pro_plus,
        ent_plus
    FROM {{source('vini_staging', 'stg_product')}}
),

cust as(
    SELECT
        id,
        TO_DATE(REPLACE(score_date, '\', '-'), 'DD-MM-YYYY') as score_date,
        csat_score
    FROM {{source('vini_public', 'customer_feedback')}}
),

Gojo as (
    SELECT 
        f.id,
        f.score_date,
        f.team_plus,
        f.pro_plus,
        f.ent_plus,
        s.csat_score
    FROM prod as f
    JOIN cust as s ON f.id = s.id and f.score_date = s.score_date
),

Geto as (
    SELECT *,
        CASE
            WHEN ent_plus = 1 THEN 3
            WHEN pro_plus = 1 THEN 2
            WHEN team_plus = 1 THEN 1 ELSE 0
        END as sub_type_id
    FROM Gojo
),

Toji as (
    SELECT
        id, 
        sub_type_id,
        AVG(csat_score) as csat_score
    FROM Geto
    GROUP BY id, sub_type_id
)

Sukuna as(
    SELECT
        sub_type_id,
        AVG(csat_score) as avg_csat_score
    FROM Toji
    GROUP BY sub_type_id
)


SELECT *,
    CASE 
        WHEN sub_type_id = 1 THEN 'team_plus'
        WHEN sub_type_id = 2 THEN 'pro_plus'
        WHEN sub_type_id = 3 THEN 'ent_plus'
        WHEN sub_type_id = 0 THEN 'No Service'
    END as sub_type
FROM Sukuna
ORDER BY avg_csat_score DESC

WITH Vini as (
    SELECT 
        id,
        TO_DATE(REPLACE(score_date, '\', '-'), 'DD-MM-YYYY') as score_date,
        csat_score
)
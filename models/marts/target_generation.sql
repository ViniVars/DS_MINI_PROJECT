WITH cust as (
    SELECT
        *
    FROM {{source('vini_public', 'customer')}}
),

temp_month as (
    SELECT
        id,
        EXTRACT(MONTH FROM TO_DATE(REPLACE(score_date, '/', '-'), 'DD-MM-YYYY')) AS month_date
    FROM cust
),

cust_month as (
    SELECT
        id, 
        month_date
    FROM temp_month
    GROUP BY id, month_date
    ORDER BY id, month_date
),


ref_cust_month as (
    SELECT *
    FROM cust_month
),

final as (

    SELECT 
        id,
        TO_DATE(REPLACE(score_date, '/', '-'), 'DD-MM-YYYY') as score_date,
        CASE 
            WHEN CAST(EXTRACT(MONTH FROM TO_DATE(REPLACE(t1.score_date, '/', '-'), 'DD-MM-YYYY')) AS INT) - 1 = 0 
            THEN 'No'
            WHEN EXISTS (
                SELECT 1
                FROM ref_cust_month as t2
                WHERE t2.id = t1.id  AND CAST(EXTRACT(MONTH FROM TO_DATE(REPLACE(t1.score_date, '/', '-'), 'DD-MM-YYYY')) AS INT) - 1 = CAST(t2.month_date as INT)
            )
            THEN 'No'
            ELSE 'Yes'
        END as churned
    FROM cust as t1
    ORDER BY id, TO_DATE(REPLACE(score_date, '/', '-'), 'DD-MM-YYYY')
)

SELECT *
FROM final
WHERE churned = 'Yes'













{{ config(materialized='table') }}

WITH base AS (
    SELECT *
    FROM {{ ref('regional_home_values') }}
),

volatility AS (
    SELECT
        region_id,
        STDDEV(yoy_growth_rate) AS price_volatility
    FROM base
    GROUP BY 1
)

SELECT
    b.*,
    v.price_volatility,
    CASE 
        WHEN v.price_volatility < 0.05 THEN 'Low'
        WHEN v.price_volatility < 0.10 THEN 'Medium'
        ELSE 'High'
    END AS volatility_category
FROM base b
LEFT JOIN volatility v ON b.region_id = v.region_id

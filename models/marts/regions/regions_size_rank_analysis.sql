{{ config(materialized='table') }}

WITH base AS (
    SELECT *
    FROM {{ ref('regional_home_values') }}
),

size_rank_metrics AS (
    SELECT
        size_rank,
        AVG(home_value) AS avg_home_value,
        AVG(yoy_growth_rate) AS avg_growth_rate,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY home_value) AS median_home_value
    FROM base
    GROUP BY 1
)

SELECT
    b.*,
    srm.avg_home_value AS size_rank_avg_value,
    srm.avg_growth_rate AS size_rank_avg_growth,
    srm.median_home_value AS size_rank_median_value,
    (b.home_value - srm.avg_home_value) / srm.avg_home_value AS value_diff_from_rank_avg
FROM base b
LEFT JOIN size_rank_metrics srm ON b.size_rank = srm.size_rank

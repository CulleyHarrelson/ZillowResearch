{{ config(materialized='table') }}

WITH base AS (
    SELECT *
    FROM {{ ref('regions_home_values') }}
),

state_metrics AS (
    SELECT
        state,
        AVG(home_value) AS state_avg_home_value,
        AVG(yoy_growth_rate) AS state_avg_growth_rate
    FROM base
    GROUP BY 1
),

metro_metrics AS (
    SELECT
        metro,
        AVG(home_value) AS metro_avg_home_value,
        AVG(yoy_growth_rate) AS metro_avg_growth_rate
    FROM base
    GROUP BY 1
)

SELECT
    b.*,
    sm.state_avg_home_value,
    sm.state_avg_growth_rate,
    mm.metro_avg_home_value,
    mm.metro_avg_growth_rate,
    (b.home_value - sm.state_avg_home_value) / sm.state_avg_home_value AS state_value_diff_pct,
    (b.home_value - mm.metro_avg_home_value) / mm.metro_avg_home_value AS metro_value_diff_pct
FROM base b
LEFT JOIN state_metrics sm ON b.state = sm.state
LEFT JOIN metro_metrics mm ON b.metro = mm.metro

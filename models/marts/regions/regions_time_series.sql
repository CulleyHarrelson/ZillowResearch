{{ config(materialized='table') }}

WITH base AS (
    SELECT *
    FROM {{ ref('regions_home_values') }}
)

SELECT
    region_id,
    region_name,
    region_type,
    state,
    city,
    metro,
    county_name,
    year_number,
    AVG(home_value) AS avg_home_value,
    MAX(home_value) AS max_home_value,
    MIN(home_value) AS min_home_value,
    AVG(yoy_growth_rate) AS avg_yoy_growth_rate
FROM base
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8

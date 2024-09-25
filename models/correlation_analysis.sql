{{ config(materialized='table') }}

WITH base_data AS (
    SELECT
        'city' AS level,
        group_id,
        group_name,
        state,
        metro,
        county_name,
        report_year,
        average_home_value,
        average_rental_value
    FROM {{ ref('city_metrics') }}
    WHERE average_home_value IS NOT NULL AND average_rental_value IS NOT NULL

    UNION ALL

    SELECT
        level,
        group_id,
        group_name,
        CASE WHEN level = 'state' THEN group_name ELSE NULL END AS state,
        CASE WHEN level = 'metro' THEN group_name ELSE NULL END AS metro,
        CASE WHEN level = 'county' THEN group_name ELSE NULL END AS county_name,
        report_year,
        average_home_value,
        average_rental_value
    FROM {{ ref('city_metrics') }}
    WHERE level IN ('county', 'state', 'metro')
        AND average_home_value IS NOT NULL AND average_rental_value IS NOT NULL
),
group_correlations AS (
    SELECT
        level,
        group_id,
        group_name,
        state,
        metro,
        county_name,
        CORR(average_home_value, average_rental_value) AS home_rental_correlation
    FROM base_data
    GROUP BY level, group_id, group_name, state, metro, county_name
),
yearly_correlations AS (
    SELECT
        level,
        report_year,
        CORR(average_home_value, average_rental_value) AS yearly_home_rental_correlation
    FROM base_data
    GROUP BY level, report_year
)
SELECT
    gc.level,
    gc.group_id,
    gc.group_name,
    gc.state,
    gc.metro,
    gc.county_name,
    gc.home_rental_correlation AS group_home_rental_correlation,
    yc.report_year,
    yc.yearly_home_rental_correlation
FROM group_correlations gc
CROSS JOIN yearly_correlations yc
WHERE gc.level = yc.level
ORDER BY 
    gc.level,
    gc.group_name,
    yc.report_year

{{ config(materialized='table') }}

WITH base_data AS (
    SELECT
        'region' AS level,
        group_id,
        group_name,
        state,
        metro,
        county_name,
        report_year,
        average_home_value,
        average_rental_value
    FROM {{ ref('region_metrics') }}
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
    FROM {{ ref('region_metrics') }}
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
        CORR(

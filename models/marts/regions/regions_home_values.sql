{{ config(materialized='table') }}

WITH home_values AS (
    SELECT *
    FROM {{ ref('int_regions_home_values_cleaned') }}
),

regions AS (
    SELECT *
    FROM {{ ref('regions') }}
),

date_spine AS (
    SELECT *
    FROM {{ ref('regions_date_spine') }}
),

previous_year_values AS (
    SELECT
        region_id,
        home_value_date,
        home_value,
        LAG(home_value, 12) OVER (PARTITION BY region_id ORDER BY home_value_date) AS previous_year_value
    FROM home_values
),

final AS (
    SELECT
        hv.region_id,
        r.region_name,
        r.region_type,
        r.state,
        r.city,
        r.metro,
        r.county_name,
        r.size_rank,
        ds.month_end_date,
        ds.year_number,
        ds.month_number,
        ds.quarter_number,
        ds.season_name,
        hv.home_value,
        pyv.previous_year_value,
        CASE
            WHEN pyv.previous_year_value IS NOT NULL AND pyv.previous_year_value != 0 
            THEN (hv.home_value - pyv.previous_year_value) / pyv.previous_year_value 
            ELSE NULL
        END AS yoy_growth_rate
    FROM home_values AS hv
    JOIN regions r ON hv.region_id = r.region_id
    JOIN date_spine ds ON hv.home_value_date = ds.month_end_date
    LEFT JOIN previous_year_values pyv ON hv.region_id = pyv.region_id AND hv.home_value_date = pyv.home_value_date
)

SELECT * FROM final

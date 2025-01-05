{{ config(materialized='view') }}

SELECT DISTINCT
    regionid AS region_id,
    metric_date AS home_value_date,
    metric_value AS home_value
FROM {{ source('raw_data', 'regions_home_values') }}
WHERE metric_value IS NOT NULL 

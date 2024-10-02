{{ config(materialized='view') }}

SELECT DISTINCT
    regionid AS region_id,
    metric_date AS home_value_date,
    metric_value AS home_value
FROM {{ source('zillow_research', 'raw_home_values') }}

{{ config(materialized='table') }}

SELECT 
    {{ star_column_group(from=ref('stg_region_home_values_raw'), dates=False) }}
FROM {{ ref('stg_region_home_values_raw') }}

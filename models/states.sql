{{ config(materialized='table') }}

SELECT DISTINCT
    "State" AS state_id
FROM (
    SELECT DISTINCT 
        "State"
    FROM {{ ref('stg_city_home_values') }} 
    WHERE "State" IS NOT NULL

    UNION ALL

    SELECT DISTINCT 
        "State"
    FROM {{ ref('stg_city_rentals') }}
    WHERE "State" IS NOT NULL
) AS states_combined

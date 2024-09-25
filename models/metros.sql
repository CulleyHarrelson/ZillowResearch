{{ config(materialized='table') }}

SELECT DISTINCT
    metro AS metro_id,
    metro AS metro_name,
    state
FROM (
    SELECT DISTINCT 
        "Metro" AS metro,
        "State" AS state
    FROM {{ ref('stg_city_home_values') }} 
    WHERE "Metro" IS NOT NULL

    UNION ALL

    SELECT DISTINCT 
        "Metro" AS metro,
        "State" AS state
    FROM {{ ref('stg_city_rentals') }}
    WHERE "Metro" IS NOT NULL
) AS metros_combined

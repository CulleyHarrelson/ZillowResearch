{{ config(materialized='table') }}

SELECT DISTINCT
    city_id,
    size_rank,
    city_name,
    state,
    metro,
    county_name
FROM (
    SELECT DISTINCT 
        "RegionID" AS city_id,
        "SizeRank" AS size_rank,
        "RegionName" AS city_name,
        "State" AS state,
        "Metro" AS metro,
        "CountyName" AS county_name
    FROM {{ ref('stg_city_home_values') }} 

    UNION ALL

    SELECT DISTINCT 
        "RegionID" AS city_id,
        "SizeRank" AS size_rank,
        "RegionName" AS city_name,
        "State" AS state,
        "Metro" AS metro,
        "CountyName" AS county_name
    FROM {{ ref('stg_city_rentals') }}
) AS cities_combined

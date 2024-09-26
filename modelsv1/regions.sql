{{ config(materialized='table') }}

SELECT DISTINCT
    region_id,
    size_rank,
    region_name,
    state,
    metro,
    county_name
FROM (
    SELECT DISTINCT 
        "RegionID" AS region_id,
        "SizeRank" AS size_rank,
        "RegionName" AS region_name,
        "State" AS state,
        "Metro" AS metro,
        "CountyName" AS county_name
    FROM {{ ref('stg_city_home_values') }} 

    UNION ALL

    SELECT DISTINCT 
        "RegionID" AS region_id,
        "SizeRank" AS size_rank,
        "RegionName" AS region_name,
        "State" AS state,
        "Metro" AS metro,
        "CountyName" AS county_name
    FROM {{ ref('stg_city_rentals') }}
) AS cities_combined

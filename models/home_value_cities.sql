{{ config(materialized='view') }}

select distinct 
    "RegionID" as city_id,
    "SizeRank" as size_rank,
    "RegionName" as city_name,
    "State" as state,
    "Metro" as metro,
    "CountyName" as county_name
from {{ ref('stg_city_home_values') }} 


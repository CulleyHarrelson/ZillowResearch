{{ config(materialized='view') }}

SELECT DISTINCT
    "RegionID" as region_id,
    "SizeRank" as size_rank,
    "RegionName" as region_name,
    "RegionType" as region_type,
    "StateName" as state_name,
    {{ dbt_utils.star(from=source('raw', 'region_home_value_forecasts_smooth'), except=["BaseDate", "RegionID", "SizeRank", "RegionName", "RegionType", "StateName"]) }}
FROM {{ source('raw', 'region_home_value_forecasts_smooth') }}

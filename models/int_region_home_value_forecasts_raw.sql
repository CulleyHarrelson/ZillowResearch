{{ config(materialized='view') }}

SELECT DISTINCT
    "RegionID" as region_id,
    "SizeRank" as size_rank,
    "RegionName" as region_name,
    "RegionType" as region_type,
    "StateName" as state_name,
    "BaseDate" as base_date,
    {{ dbt_utils.star(from=ref('stg_region_home_value_forecasts_raw'), except=["BaseDate", "RegionID", "SizeRank", "RegionName", "RegionType", "StateName"]) }}
FROM {{ ref('stg_region_home_value_forecasts_raw') }}

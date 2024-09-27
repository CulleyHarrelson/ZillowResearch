{{ config(materialized='table') }}

WITH unioned_data AS (
    {{ dbt_utils.union_relations(
        relations=[
            ref('stg_region_home_value_forecasts_raw'),
            ref('stg_region_home_value_forecasts_smooth'),
            ref('stg_region_home_values_raw'),
            ref('stg_region_home_values_smooth'),
            ref('stg_region_rentals'),
            ref('stg_region_renter_demand')
        ],
        source_column_name=None,
        include=[
            "RegionID",
            "SizeRank",
            "RegionName",
            "RegionType",
            "StateName"
        ]
    ) }}
)

SELECT DISTINCT
    "RegionID" as region_id,
    "SizeRank" as size_rank,
    "RegionName" as region_name,
    "RegionType" as region_type,
    "StateName" as state_name
FROM unioned_data

{{ config(materialized='table') }}

WITH unioned_data AS (
    {{ dbt_utils.union_relations(
        relations=[
            ref('stg_region_home_value_forecasts'),
            ref('stg_region_home_values'),
            ref('stg_region_rentals'),
            ref('stg_region_renter_demand')
        ],
        source_column_name=None,
        include=[
            "region_id",
            "size_rank",
            "region_name",
            "region_type",
            "state_name"
        ]
    ) }}
)

SELECT DISTINCT
    region_id,
    size_rank,
    region_name,
    region_type,
    state_name
FROM unioned_data

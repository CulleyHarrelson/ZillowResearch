{{ config(materialized='table') }}

WITH unioned_data AS (
    {{ dbt_utils.union_relations(
        relations=[
            ref('int_region_home_value_forecasts_pivoted'),
            ref('int_region_home_values_pivoted'),
            ref('int_region_rentals_pivoted'),
            ref('int_region_renter_demand_pivoted')
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

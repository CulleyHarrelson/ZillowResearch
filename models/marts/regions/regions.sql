{{ config(materialized='table') }}

WITH unioned_data AS (
    {{ dbt_utils.union_relations(
        relations=[
            ref('home_values'),
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

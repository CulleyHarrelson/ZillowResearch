{{ config(materialized='view') }}

    select RegionID
    from {{ source('awsdatacatalog', 'raw_home_values') }}


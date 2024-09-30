{{ config(materialized='view') }}

    select *
    from {{ source('awsdatacatalog', 'raw_home_values') }}


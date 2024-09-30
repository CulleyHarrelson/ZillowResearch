{{ config(materialized='view') }}

    select *
    from {{ source('awsdatacatalog', 'zillow_research') }}


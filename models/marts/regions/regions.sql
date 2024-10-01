{{ config(materialized='table') }}

SELECT DISTINCT
    regionid as region_id,
    sizerank as size_rank,
    regionname as region_name,
    regiontype as region_type,
    statename as state_name
FROM {{ source('zillow_research', 'raw_regions') }}

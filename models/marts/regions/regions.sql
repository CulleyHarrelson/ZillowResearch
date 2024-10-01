{{ config(materialized='table') }}

SELECT DISTINCT
    regionid AS region_id,
    sizerank AS size_rank,
    regionname AS region_name,
    regiontype AS region_type,
    statename AS state_name,
    state,
    metro,
    countyname AS count_name,
    city,
    statecodefips AS state_code_fips,
    municipalcodefips AS municipal_code_fips
FROM {{ source('zillow_research', 'raw_regions') }}

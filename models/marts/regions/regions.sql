{{ config(materialized='table') }}

SELECT DISTINCT
    regionid AS region_id,
    sizerank AS size_rank,
    REGEXP_REPLACE(regionname, '^0+', '') AS region_name,
    regiontype AS region_type,
    statename AS state_name,
    state,
    metro,
    countyname AS count_name,
    city,
    REGEXP_REPLACE(statecodefips, '\.0$', '') AS state_code_fips,
    municipalcodefips AS municipal_code_fips
FROM {{ source('zillow_research', 'raw_regions') }}
WHERE metro not like '"%'

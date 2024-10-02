{{ config(materialized='view') }}

SELECT DISTINCT
    regionid AS region_id,
    sizerank AS size_rank,
    CASE
        WHEN regiontype = 'zip' THEN regionname
        ELSE REGEXP_REPLACE(regionname, '^0+', '')
    END AS region_name,
    regiontype AS region_type,
    statename AS state_name,
    state,
    metro,
    countyname AS county_name,
    city,
    REGEXP_REPLACE(statecodefips, '\.0$', '') AS state_code_fips,
    municipalcodefips AS municipal_code_fips
FROM {{ source('zillow_research', 'raw_regions') }}
WHERE metro NOT LIKE '"%'

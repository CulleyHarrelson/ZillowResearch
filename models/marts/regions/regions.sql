
{{ config(materialized='table') }}

SELECT *
FROM {{ ref('int_regions_cleaned') }}

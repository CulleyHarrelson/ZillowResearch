{{ config(materialized='view') }}

select *
from {{ source('zillow_research', 'raw_home_values') }}

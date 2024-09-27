{{ config(materialized='table') }}


{{ unpivot_zillow_research_data(table_name='stg_region_rentals') }}

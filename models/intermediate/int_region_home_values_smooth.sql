{{ config(materialized='view') }}


{{ unpivot_zillow_research_data(table_name='stg_region_home_values_smooth') }}

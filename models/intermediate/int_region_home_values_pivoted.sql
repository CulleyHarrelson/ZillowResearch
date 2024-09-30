{{ config(materialized='table') }}


{{ unpivot_zillow_research_data(table_name='raw_home_values') }}

{{ config(materialized='view') }}


{{ unpivot_zillow_research_data(table_name='region_home_value_forecasts') }}

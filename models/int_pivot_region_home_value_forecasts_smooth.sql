{{ config(materialized='table') }}


{{ unpivot_values(table_name='int_region_home_value_forecasts_smooth') }}

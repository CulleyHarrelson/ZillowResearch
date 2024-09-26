{{ config(materialized='table') }}


{{ unpivot_values(table_name='int_region_home_values_property_types') }}

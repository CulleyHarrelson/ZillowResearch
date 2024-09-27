{{ config(materialized='table') }}


{{ unpivot_values(table_name='stg_region_renter_demand') }}

{{ config(materialized='table') }}
{{ config(materialized='table') }}

WITH date_spine AS (
    SELECT DISTINCT home_value_date AS month_end_date
    FROM {{ ref('int_regions_home_values_cleaned') }}
)
SELECT
    month_end_date,
    DATE_TRUNC(month_end_date, MONTH) AS month_start_date,
    FORMAT_DATE('%Y-%m', month_end_date) AS month_key,
    EXTRACT(YEAR FROM month_end_date) AS year_number,
    EXTRACT(MONTH FROM month_end_date) AS month_number,
    FORMAT_DATE('%B', month_end_date) AS month_full_name,
    FORMAT_DATE('%b', month_end_date) AS month_short_name,
    EXTRACT(QUARTER FROM month_end_date) AS quarter_number,
    DATE_TRUNC(month_end_date, QUARTER) AS quarter_start_date,
    DATE_TRUNC(month_end_date, YEAR) AS year_start_date,
    CASE 
        WHEN EXTRACT(MONTH FROM month_end_date) IN (12, 1, 2) THEN 'Winter'
        WHEN EXTRACT(MONTH FROM month_end_date) IN (3, 4, 5) THEN 'Spring'
        WHEN EXTRACT(MONTH FROM month_end_date) IN (6, 7, 8) THEN 'Summer'
        ELSE 'Fall'
    END AS season_name,
    CAST(EXTRACT(YEAR FROM month_end_date) * 100 + EXTRACT(MONTH FROM month_end_date) AS INT64) AS year_month_int
FROM date_spine
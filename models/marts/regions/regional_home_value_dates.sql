{{ config(materialized='table') }}

WITH date_spine AS (
    SELECT DISTINCT home_value_date AS month_end_date
    FROM {{ ref('home_values') }}
)
SELECT
    month_end_date,
    DATE_TRUNC('month', month_end_date) AS month_start_date,
    format_datetime(month_end_date, 'yyyy-MM') AS month_key,
    CAST(year(month_end_date) AS INTEGER) AS year_number,
    CAST(month(month_end_date) AS INTEGER) AS month_number,
    format_datetime(month_end_date, 'MMMM') AS month_full_name,
    format_datetime(month_end_date, 'MMM') AS month_short_name,
    CAST(quarter(month_end_date) AS INTEGER) AS quarter_number,
    DATE_TRUNC('quarter', month_end_date) AS quarter_start_date,
    DATE_TRUNC('year', month_end_date) AS year_start_date,
    CASE 
        WHEN month(month_end_date) IN (12, 1, 2) THEN 'Winter'
        WHEN month(month_end_date) IN (3, 4, 5) THEN 'Spring'
        WHEN month(month_end_date) IN (6, 7, 8) THEN 'Summer'
        ELSE 'Fall'
    END AS season_name,
    CAST(year(month_end_date) * 100 + month(month_end_date) AS INTEGER) AS year_month_int
FROM date_spine

{{ config(materialized='table') }}

WITH date_spine AS (
    SELECT DISTINCT home_value_date AS month_end_date
    FROM {{ ref('int_home_values') }}
)
SELECT
    month_end_date,
    DATE_TRUNC('month', month_end_date) AS month_start_date,
    TO_CHAR(month_end_date, 'YYYY-MM') AS month_key,
    EXTRACT(YEAR FROM month_end_date) AS year,
    EXTRACT(MONTH FROM month_end_date) AS month,
    TO_CHAR(month_end_date, 'Month') AS month_name,
    TO_CHAR(month_end_date, 'Mon') AS month_short_name,
    EXTRACT(QUARTER FROM month_end_date) AS quarter,
    DATE_TRUNC('quarter', month_end_date) AS quarter_start_date,
    DATE_TRUNC('year', month_end_date) AS year_start_date,
    CASE 
        WHEN EXTRACT(MONTH FROM month_end_date) IN (12, 1, 2) THEN 'Winter'
        WHEN EXTRACT(MONTH FROM month_end_date) IN (3, 4, 5) THEN 'Spring'
        WHEN EXTRACT(MONTH FROM month_end_date) IN (6, 7, 8) THEN 'Summer'
        ELSE 'Fall'
    END AS season,
    EXTRACT(YEAR FROM month_end_date) * 100 + EXTRACT(MONTH FROM month_end_date) AS year_month_int
FROM date_spine

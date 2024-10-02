{{ config(materialized='table') }}

WITH date_spine AS (
    SELECT DISTINCT home_value_date AS date
    FROM {{ ref('home_values') }}
)
SELECT
    date,
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(MONTH FROM date) AS month,
    EXTRACT(DAY FROM date) AS day,
    DATE_TRUNC('month', date) AS month_start,
    DATE_TRUNC('quarter', date) AS quarter_start,
    DATE_TRUNC('year', date) AS year_start
FROM date_spine

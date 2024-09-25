{{ config(materialized='table') }}

WITH city_size_ranks AS (
    SELECT
        city_id,
        city_name,
        state,
        metro,
        size_rank AS current_size_rank
    FROM {{ ref('cities') }}
),
yearly_metrics AS (
    SELECT
        city_id,
        report_year,
        average_home_value,
        average_rental_value
    FROM {{ ref('city_metrics') }}
),
yearly_size_ranks AS (
    SELECT
        city_id,
        report_year,
        ROW_NUMBER() OVER (
            PARTITION BY report_year 
            ORDER BY COALESCE(average_home_value, 0) DESC, COALESCE(average_rental_value, 0) DESC
        ) AS calculated_size_rank
    FROM yearly_metrics
),
size_rank_changes AS (
    SELECT
        y.city_id,
        y.report_year,
        y.calculated_size_rank,
        LAG(y.calculated_size_rank) OVER (PARTITION BY y.city_id ORDER BY y.report_year) AS previous_year_rank,
        c.current_size_rank
    FROM yearly_size_ranks y
    JOIN city_size_ranks c ON y.city_id = c.city_id
)
SELECT
    s.city_id,
    c.city_name,
    c.state,
    c.metro,
    s.report_year,
    s.current_size_rank,
    s.calculated_size_rank,
    s.previous_year_rank,
    s.calculated_size_rank - s.previous_year_rank AS rank_change_from_previous_year,
    s.current_size_rank - s.calculated_size_rank AS rank_change_from_current
FROM size_rank_changes s
JOIN city_size_ranks c ON s.city_id = c.city_id
ORDER BY s.city_id, s.report_year

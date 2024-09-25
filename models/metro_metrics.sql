{{ config(materialized='table') }}

WITH home_values_yearly AS (
    SELECT
        metro AS metro_id,
        EXTRACT(YEAR FROM value_date)::INTEGER AS report_year,
        AVG(home_value) AS average_home_value
    FROM {{ ref('home_values_by_city') }}
    JOIN {{ ref('cities') }} USING (city_id)
    WHERE home_value IS NOT NULL AND metro IS NOT NULL
    GROUP BY metro, EXTRACT(YEAR FROM value_date)::INTEGER
),
rentals_yearly AS (
    SELECT
        metro AS metro_id,
        EXTRACT(YEAR FROM value_date)::INTEGER AS report_year,
        AVG(rental_value) AS average_rental_value
    FROM {{ ref('rentals_by_city') }}
    JOIN {{ ref('cities') }} USING (city_id)
    WHERE rental_value IS NOT NULL AND metro IS NOT NULL
    GROUP BY metro, EXTRACT(YEAR FROM value_date)::INTEGER
),
year_over_year_changes AS (
    SELECT
        metro_id,
        report_year,
        average_home_value,
        LAG(average_home_value) OVER (PARTITION BY metro_id ORDER BY report_year) AS previous_year_home_value,
        average_rental_value,
        LAG(average_rental_value) OVER (PARTITION BY metro_id ORDER BY report_year) AS previous_year_rental_value
    FROM (
        SELECT 
            COALESCE(home_values.metro_id, rentals.metro_id) AS metro_id,
            COALESCE(home_values.report_year, rentals.report_year) AS report_year,
            home_values.average_home_value,
            rentals.average_rental_value
        FROM home_values_yearly AS home_values
        FULL OUTER JOIN rentals_yearly AS rentals 
            ON home_values.metro_id = rentals.metro_id 
            AND home_values.report_year = rentals.report_year
    ) AS combined_values
)
SELECT
    metros.metro_id,
    metros.metro_name,
    metros.state,
    home_values.report_year,
    home_values.average_home_value,
    rentals.average_rental_value,
    CASE 
        WHEN year_over_year.previous_year_home_value IS NOT NULL 
        AND year_over_year.previous_year_home_value != 0
        THEN (year_over_year.average_home_value - year_over_year.previous_year_home_value) 
            / year_over_year.previous_year_home_value * 100 
        ELSE NULL 
    END AS home_value_year_over_year_change_percent,
    CASE 
        WHEN year_over_year.previous_year_rental_value IS NOT NULL 
        AND year_over_year.previous_year_rental_value != 0
        THEN (year_over_year.average_rental_value - year_over_year.previous_year_rental_value) 
            / year_over_year.previous_year_rental_value * 100 
        ELSE NULL 
    END AS rental_value_year_over_year_change_percent,
    CASE 
        WHEN rentals.average_rental_value IS NOT NULL 
        AND rentals.average_rental_value != 0
        THEN home_values.average_home_value / (rentals.average_rental_value * 12)
        ELSE NULL 
    END AS price_to_annual_rent_ratio
FROM {{ ref('metros') }} AS metros
LEFT JOIN home_values_yearly AS home_values ON metros.metro_id = home_values.metro_id
LEFT JOIN rentals_yearly AS rentals 
    ON metros.metro_id = rentals.metro_id 
    AND home_values.report_year = rentals.report_year
LEFT JOIN year_over_year_changes AS year_over_year 
    ON metros.metro_id = year_over_year.metro_id 
    AND home_values.report_year = year_over_year.report_year
WHERE (home_values.average_home_value IS NOT NULL OR rentals.average_rental_value IS NOT NULL)
ORDER BY metros.metro_id, home_values.report_year

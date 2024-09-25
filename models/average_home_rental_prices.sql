{{ config(materialized='table') }}

WITH home_values_yearly AS (
    SELECT
        city_id,
        EXTRACT(YEAR FROM value_date)::INTEGER AS year,
        AVG(home_value) AS average_home_value
    FROM {{ ref('home_values_by_city') }}
    WHERE home_value IS NOT NULL
    GROUP BY 1, 2
),
rentals_yearly AS (
    SELECT
        city_id,
        EXTRACT(YEAR FROM value_date)::INTEGER AS year,
        AVG(rental_value) AS average_rental_value
    FROM {{ ref('rentals_by_city') }}
    WHERE rental_value IS NOT NULL
    GROUP BY 1, 2
)
SELECT
    cities.city_id,
    cities.city_name,
    cities.state,
    cities.metro,
    home_values.year,
    home_values.average_home_value,
    rentals.average_rental_value
FROM {{ ref('cities') }} AS cities
LEFT JOIN home_values_yearly AS home_values ON cities.city_id = home_values.city_id
LEFT JOIN rentals_yearly AS rentals ON cities.city_id = rentals.city_id AND home_values.year = rentals.year
WHERE (home_values.average_home_value IS NOT NULL OR rentals.average_rental_value IS NOT NULL)
ORDER BY cities.city_id, home_values.year

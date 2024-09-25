{{ config(materialized='table') }}

WITH home_values_yearly AS (
    SELECT
        city_id,
        EXTRACT(YEAR FROM value_date)::INTEGER AS report_year,
        AVG(home_value) AS average_home_value
    FROM {{ ref('home_values_by_city') }}
    WHERE home_value IS NOT NULL
    GROUP BY city_id, EXTRACT(YEAR FROM value_date)::INTEGER
),
rentals_yearly AS (
    SELECT
        city_id,
        EXTRACT(YEAR FROM value_date)::INTEGER AS report_year,
        AVG(rental_value) AS average_rental_value
    FROM {{ ref('rentals_by_city') }}
    WHERE rental_value IS NOT NULL
    GROUP BY city_id, EXTRACT(YEAR FROM value_date)::INTEGER
),
year_over_year_changes AS (
    SELECT
        city_id,
        report_year,
        average_home_value,
        LAG(average_home_value) OVER (PARTITION BY city_id ORDER BY report_year) AS previous_year_home_value,
        average_rental_value,
        LAG(average_rental_value) OVER (PARTITION BY city_id ORDER BY report_year) AS previous_year_rental_value
    FROM (
        SELECT 
            COALESCE(home_values.city_id, rentals.city_id) AS city_id,
            COALESCE(home_values.report_year, rentals.report_year) AS report_year,
            home_values.average_home_value,
            rentals.average_rental_value
        FROM home_values_yearly AS home_values
        FULL OUTER JOIN rentals_yearly AS rentals 
            ON home_values.city_id = rentals.city_id 
            AND home_values.report_year = rentals.report_year
    ) AS combined_values
),
city_metrics AS (
    SELECT
        cities.city_id,
        cities.city_name,
        cities.state,
        cities.metro,
        cities.county_name,
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
    FROM {{ ref('cities') }} AS cities
    LEFT JOIN home_values_yearly AS home_values ON cities.city_id = home_values.city_id
    LEFT JOIN rentals_yearly AS rentals 
        ON cities.city_id = rentals.city_id 
        AND home_values.report_year = rentals.report_year
    LEFT JOIN year_over_year_changes AS year_over_year 
        ON cities.city_id = year_over_year.city_id 
        AND home_values.report_year = year_over_year.report_year
    WHERE (home_values.average_home_value IS NOT NULL OR rentals.average_rental_value IS NOT NULL)
),
aggregated_metrics AS (
    SELECT
        'metro' AS level,
        metro::TEXT AS group_id,
        metro AS group_name,
        report_year,
        AVG(average_home_value) AS average_home_value,
        AVG(average_rental_value) AS average_rental_value,
        AVG(home_value_year_over_year_change_percent) AS home_value_year_over_year_change_percent,
        AVG(rental_value_year_over_year_change_percent) AS rental_value_year_over_year_change_percent,
        AVG(price_to_annual_rent_ratio) AS price_to_annual_rent_ratio
    FROM city_metrics
    WHERE metro IS NOT NULL
    GROUP BY metro, report_year

    UNION ALL

    SELECT
        'state' AS level,
        state::TEXT AS group_id,
        state AS group_name,
        report_year,
        AVG(average_home_value) AS average_home_value,
        AVG(average_rental_value) AS average_rental_value,
        AVG(home_value_year_over_year_change_percent) AS home_value_year_over_year_change_percent,
        AVG(rental_value_year_over_year_change_percent) AS rental_value_year_over_year_change_percent,
        AVG(price_to_annual_rent_ratio) AS price_to_annual_rent_ratio
    FROM city_metrics
    WHERE state IS NOT NULL
    GROUP BY state, report_year

    UNION ALL

    SELECT
        'county' AS level,
        county_name::TEXT AS group_id,
        county_name AS group_name,
        report_year,
        AVG(average_home_value) AS average_home_value,
        AVG(average_rental_value) AS average_rental_value,
        AVG(home_value_year_over_year_change_percent) AS home_value_year_over_year_change_percent,
        AVG(rental_value_year_over_year_change_percent) AS rental_value_year_over_year_change_percent,
        AVG(price_to_annual_rent_ratio) AS price_to_annual_rent_ratio
    FROM city_metrics
    WHERE county_name IS NOT NULL
    GROUP BY county_name, report_year
)
SELECT *
FROM (
    SELECT
        'city' AS level,
        city_id::TEXT AS group_id,
        city_name AS group_name,
        state,
        metro,
        county_name,
        report_year,
        average_home_value,
        average_rental_value,
        home_value_year_over_year_change_percent,
        rental_value_year_over_year_change_percent,
        price_to_annual_rent_ratio
    FROM city_metrics

    UNION ALL

    SELECT
        level,
        group_id,
        group_name,
        NULL AS state,
        CASE WHEN level = 'metro' THEN group_name ELSE NULL END AS metro,
        CASE WHEN level = 'county' THEN group_name ELSE NULL END AS county_name,
        report_year,
        average_home_value,
        average_rental_value,
        home_value_year_over_year_change_percent,
        rental_value_year_over_year_change_percent,
        price_to_annual_rent_ratio
    FROM aggregated_metrics
) AS combined_data
ORDER BY 
    CASE 
        WHEN level = 'city' THEN 1 
        WHEN level = 'county' THEN 2
        WHEN level = 'metro' THEN 3
        WHEN level = 'state' THEN 4
    END,
    group_name, 
    report_year

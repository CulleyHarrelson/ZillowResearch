-- depends_on: {{ ref('stg_city_rentals') }}
{{ config(
    materialized='table',
    indexes=[
        {'columns': ['city_id', 'value_date']}
    ]
) }}

{% set date_columns = get_city_rentals_date_columns() %}

WITH unpivoted_rental_data AS (
  {% for date_column in date_columns %}
  SELECT
    "RegionID" AS city_id,
    '{{ date_column }}' AS value_date,
    "{{ date_column }}" AS rental_value
  FROM {{ ref('stg_city_rentals') }}
  {% if not loop.last %}
  UNION ALL
  {% endif %}
  {% endfor %}
)
SELECT 
  cities.city_id,
  CAST(unpivoted_rental_data.value_date AS DATE) AS value_date,
  unpivoted_rental_data.rental_value
FROM {{ ref('cities') }} AS cities
JOIN unpivoted_rental_data ON cities.city_id = unpivoted_rental_data.city_id
ORDER BY cities.city_id, unpivoted_rental_data.value_date

-- depends_on: {{ ref('stg_city_home_values') }}
{{ config(
    materialized='table',
    indexes=[
        {'columns': ['city_id', 'value_date']}
    ]
) }}

{% set date_columns = get_city_home_values_date_columns() %}

WITH unpivoted_home_value_data AS (
  {% for date_column in date_columns %}
  SELECT
    "RegionID" AS city_id,
    '{{ date_column }}' AS value_date,
    "{{ date_column }}" AS home_value
  FROM {{ ref('stg_city_home_values') }}
  {% if not loop.last %}
  UNION ALL
  {% endif %}
  {% endfor %}
)
SELECT 
  cities.city_id,
  CAST(unpivoted_home_value_data.value_date AS DATE) AS value_date,
  unpivoted_home_value_data.home_value
FROM {{ ref('cities') }} AS cities
JOIN unpivoted_home_value_data ON cities.city_id = unpivoted_home_value_data.city_id
ORDER BY cities.city_id, unpivoted_home_value_data.value_date

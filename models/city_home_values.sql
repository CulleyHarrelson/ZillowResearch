-- depends_on: {{ ref('stg_city_home_values') }}
{{ config(
    materialized='table',
    indexes=[
        {'columns': ['city_id', 'value_date']}
    ]
) }}

{% set date_columns = get_date_columns() %}

WITH unpivoted_data AS (
  {% for date_col in date_columns %}
  SELECT
    "RegionID" as city_id,
    '{{ date_col }}' AS value_date,
    "{{ date_col }}" AS home_value
  FROM {{ ref('stg_city_home_values') }}
  {% if not loop.last %}
  UNION ALL
  {% endif %}
  {% endfor %}
)
SELECT 
  c.city_id,
  CAST(u.value_date AS DATE) AS value_date,
  u.home_value
FROM {{ ref('home_value_cities') }} c
JOIN unpivoted_data u ON c.city_id = u.city_id
ORDER BY c.city_id, u.value_date

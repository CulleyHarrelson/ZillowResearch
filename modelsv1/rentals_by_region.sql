-- depends_on: {{ ref('stg_city_home_values') }}
{{ config(
    materialized='table',
    indexes=[
        {'columns': ['region_id', 'value_date']}
    ]
) }}

{% set date_columns = get_region_home_values_date_columns() %}

WITH unpivoted_home_value_data AS (
  {% for date_column in date_columns %}
  SELECT
    "RegionID" AS region_id,
    '{{ date_column }}' AS value_date,
    "{{ date_column }}" AS home_value
  FROM {{ ref('stg_city_home_values') }}
  {% if not loop.last %}
  UNION ALL
  {% endif %}
  {% endfor %}
)
SELECT 
  regions.region_id,
  CAST(unpivoted_home_value_data.value_date AS DATE) AS value_date,
  unpivoted_home_value_data.home_value
FROM {{ ref('regions') }} AS regions
JOIN unpivoted_home_value_data ON regions.region_id = unpivoted_home_value_data.region_id
ORDER BY regions.region_id, unpivoted_home_value_data.value_date

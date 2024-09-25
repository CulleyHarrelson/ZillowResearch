{% macro get_city_home_values_date_columns() %}
  {% set date_columns_query %}
    SELECT column_name
    FROM information_schema.columns
    WHERE table_name = 'stg_city_home_values'
    ORDER BY ordinal_position
    OFFSET 8
  {% endset %}
  
  {% set results = run_query(date_columns_query) %}
  
  {% if execute %}
    {% set date_columns = results.columns[0].values() %}
  {% else %}
    {% set date_columns = [] %}
  {% endif %}
  
  {{ return(date_columns) }}
{% endmacro %}

{% macro get_city_rentals_date_columns() %}
  {% set date_columns_query %}
    SELECT column_name
    FROM information_schema.columns
    WHERE table_name = 'stg_city_rentals'
    ORDER BY ordinal_position
    OFFSET 8
  {% endset %}
  
  {% set results = run_query(date_columns_query) %}
  
  {% if execute %}
    {% set date_columns = results.columns[0].values() %}
  {% else %}
    {% set date_columns = [] %}
  {% endif %}
  
  {{ return(date_columns) }}
{% endmacro %}

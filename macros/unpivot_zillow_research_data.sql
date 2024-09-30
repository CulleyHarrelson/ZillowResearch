{% macro unpivot_zillow_research_data(table_name) %}

{%- set source_table = source('zillow_research', table_name) -%}
{%- set column_names = dbt_utils.get_filtered_columns_in_relation(
    from=source_table,
    except=["regionid","sizerank","regionname","regiontype","statename","state","metro","countyname","city","statecodefips","municipalcodefips"]
) -%}


WITH stg_data AS (
    SELECT DISTINCT
        "regionid",
        "sizerank",
        "regionname",
        "regiontype",
        "statename",
        "state",
        "metro",
        "countyname",
        "city",
        "statecodefips",
        "municipalcodefips",
        {% for column in column_names %}
            "{{ column }}" as "{{ column }}"{% if not loop.last %},{% endif %}
        {% endfor %}
    FROM {{ source_table }}
)

{% for column in column_names %}
    select
        "regionid",
        "sizerank",
        "regionname",
        "regiontype",
        "statename",
        "state",
        "metro",
        "countyname",
        "city",
        "statecodefips",
        "municipalcodefips",
        '{{ column }}' as metric_date,
        "{{ column }}" as metric_value
    from stg_data
    {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %}
{% endmacro %}


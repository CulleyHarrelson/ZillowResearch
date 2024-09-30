{% macro unpivot_zillow_research_data(table_name) %}

{%- set source_table = source('zillow_research', table_name) -%}
{%- set columns = run_query("SELECT * FROM " ~ source_table ~ " LIMIT 0") -%}
{%- set column_names = columns.columns | reject("in", ["regionid","sizerank","regionname","regiontype","statename","state","metro","countyname","city","statecodefips","municipalcodefips","id"]) | list -%}

WITH stg_data AS (
    SELECT DISTINCT
        `regionid`,
        `sizerank`,
        `regionname`,
        `regiontype`,
        `statename`,
        `state`,
        `metro`,
        `countyname`,
        `city`,
        `statecodefips`,
        `municipalcodefips`,
        `id`,
        {% for column in column_names %}
            `{{ column }}` as `{{ column }}`{% if not loop.last %},{% endif %}
        {% endfor %}
    FROM {{ source_table }}
)

{% for column in column_names %}
    select
        `regionid`,
        `sizerank`,
        `regionname`,
        `regiontype`,
        `statename`,
        `state`,
        `metro`,
        `countyname`,
        `city`,
        `statecodefips`,
        `municipalcodefips`,
        `id`,
        '{{ column }}' as metric_date,
        `{{ column }}` as metric_value
    from stg_data
    {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %}
{% endmacro %}


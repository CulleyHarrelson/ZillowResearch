{% macro unpivot_zillow_research_data(table_name) %}
{%- set relation = ref(table_name) -%}
{%- set columns = adapter.get_columns_in_relation(relation) -%}
{%- set column_names = dbt_utils.get_filtered_columns_in_relation(
    from=relation,
    except=["region_id", "size_rank", "region_name", "region_type", "state_name", "base_date"]
) -%}
{%- set base_date_exists = 'base_date' in columns | map(attribute='name') -%}

{% for column in column_names %}
    select
        region_id,
        size_rank,
        region_name,
        region_type,
        state_name,
        {% if base_date_exists %}
        base_date,
        {% endif %}
        '{{ column }}' as metric_date,
        "{{ column }}" as metric_value
    from {{ relation }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %}
{% endmacro %}

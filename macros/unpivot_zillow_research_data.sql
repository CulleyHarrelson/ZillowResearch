{% macro unpivot_zillow_research_data(table_name) %}

{%- set source_table = source('raw', table_name) -%}
{%- set columns = adapter.get_columns_in_relation(source_table) -%}
{%- set column_names = dbt_utils.get_filtered_columns_in_relation(
    from=source_table,
    except=["RegionID", "SizeRank", "RegionName", "RegionType", "StateName"]
) -%}

WITH stg_data AS (
    SELECT DISTINCT
        "RegionID" as region_id,
        "SizeRank" as size_rank,
        "RegionName" as region_name,
        "RegionType" as region_type,
        "StateName" as state_name,
        {{ dbt_utils.star(from=source_table, except=["RegionID", "SizeRank", "RegionName", "RegionType", "StateName"]) }}
    FROM {{ source_table }}
)

{% for column in column_names %}
    select
        region_id,
        size_rank,
        region_name,
        region_type,
        state_name,
        '{{ column }}' as metric_date,
        "{{ column }}" as metric_value
    from stg_data
    {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %}
{% endmacro %}

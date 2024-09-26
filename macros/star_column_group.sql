{% macro star_column_group(from, include_dates=False) %}
    {%- set include_cols = [] %}
    
    {%- set cols = adapter.get_columns_in_relation(from) %}
    
    {%- for col in cols -%}
        {%- if include_dates -%}
            {%- if modules.re.match('^\\d{4}-\\d{2}-\\d{2}$', col.name) -%}
                {%- do include_cols.append(col.name) %}
            {%- endif %}
        {%- else -%}
            {%- if not modules.re.match('^\\d{4}-\\d{2}-\\d{2}$', col.name) -%}
                {%- do include_cols.append(col.name) %}
            {%- endif %}
        {%- endif %}
    {%- endfor %}
    
    {%- for col in include_cols %}
        "{{ col }}"{% if not loop.last %},{% endif %}
    {%- endfor %}
{% endmacro %}

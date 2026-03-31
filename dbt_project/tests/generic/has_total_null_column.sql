{% test has_total_null_column(model) %}

    {% set columns = adapter.get_columns_in_relation(model) %}
    {% set column_queries = [] %}

    {% for column in columns %}
        {% set column_query %}
        SELECT
            '{{ column.name }}' AS column_name,
            COUNT({{ column.name }}) = 0 AS is_all_null
        from {{ model }}
        {% endset %}
        {% do column_queries.append(column_query) %}
    {% endfor %}

    {% set union_query = column_queries | join(' UNION ALL ') %}
    
    with null_columns AS (
        {{ union_query }}
    )
    SELECT column_name
    FROM null_columns
    WHERE is_all_null = TRUE

{% endtest %}



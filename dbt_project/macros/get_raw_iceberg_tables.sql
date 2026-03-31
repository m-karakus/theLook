{% macro get_raw_iceberg_tables() %}
  {# Extract all raw_* table names from the 'stg' source in _raw__sources.yml #}
  {% set raw_tables = [] %}
  
  {% if execute %}
    {% for source in graph.sources.values() %}
      {% if source.source_name == 'dwh_stg' and source.name.startswith('raw_') %}
        {% do raw_tables.append(source.name) %}
      {% endif %}
    {% endfor %}
  {% endif %}
  
  {{ return(raw_tables) }}
{% endmacro %}

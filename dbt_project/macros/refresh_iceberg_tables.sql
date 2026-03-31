{% macro refresh_iceberg_tables() %}
  {# 
    Refreshes all Iceberg external tables to sync with latest S3/Glue metadata.
    Should be called after mkpipe runs and before dbt models execute.
    Called in on-run-start hook.
  #}
  
  {% set raw_tables = get_raw_iceberg_tables() %}
  {% set schema = var('raw_schema', 'DWH_STG') | upper %}
  
  {{ log("=== Refreshing Iceberg Tables ===", info=True) }}
  
  {% for table_name in raw_tables %}
    {% set refresh_sql %}
      ALTER ICEBERG TABLE {{ schema }}.{{ table_name }} REFRESH
    {% endset %}
    
    {% if execute %}
      {% do run_query(refresh_sql) %}
      {{ log("✓ Refreshed: " ~ table_name, info=True) }}
    {% endif %}
  {% endfor %}
  
  {{ log("=== Iceberg Refresh Complete ===", info=True) }}
{% endmacro %}

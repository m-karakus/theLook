{% macro create_iceberg_tables() %}
  {# 
    Idempotently creates Iceberg external tables in Snowflake for all raw_* tables.
    Checks if table exists before creating to avoid errors.
    Called in on-run-start hook.
  #}
  
  {% set raw_tables = get_raw_iceberg_tables() %}
  {% set schema = var('raw_schema', 'DWH_STG') | upper %}
  
  {{ log("=== Checking Iceberg Tables ===", info=True) }}
  {{ log("Found " ~ raw_tables | length ~ " raw tables in sources.yml", info=True) }}
  
  {% for table_name in raw_tables %}
    {% set check_query %}
      SELECT COUNT(*) as cnt 
      FROM information_schema.tables 
      WHERE table_schema = '{{ schema }}' 
        AND table_name = '{{ table_name | upper }}'
    {% endset %}
    
    {% set result = run_query(check_query) %}
    
    {% if execute and result %}
      {% set table_exists = result.rows[0][0] > 0 %}
      
      {% if not table_exists %}
        {% set create_sql %}
          CREATE OR REPLACE ICEBERG TABLE {{ schema }}.{{ table_name }}
            EXTERNAL_VOLUME = iceberg_volume
            CATALOG = glue_catalog_integration
            CATALOG_NAMESPACE = 'iceberg_db'
            CATALOG_TABLE_NAME = '{{ table_name }}'
            AUTO_REFRESH = TRUE
        {% endset %}
        
        {% do run_query(create_sql) %}
        {{ log("✓ Created Iceberg table: " ~ schema ~ "." ~ table_name, info=True) }}
      {% else %}
        {{ log("  Table exists: " ~ table_name, info=False) }}
      {% endif %}
    {% endif %}
  {% endfor %}
  
  {{ log("=== Iceberg Table Check Complete ===", info=True) }}
{% endmacro %}

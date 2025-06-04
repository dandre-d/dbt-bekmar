{% macro union_sap_tables(table_name) %}
  {% set active_dbs = [] %}
  {% set column_list = "" %}

  {% if execute %}
    {# Step 1: Get list of active databases #}
    {% set db_query = "SELECT db_name FROM control.source_db_control WHERE is_active = 1" %}
    {% set db_results = run_query(db_query) %}
    {% if db_results is not none %}
      {% set active_dbs = db_results.columns[0].values() %}
    {% endif %}

    {# Step 2: Get column list for the table_name from vw_columnStats #}
    {% set col_query %}
      SELECT column_name
      FROM control.vw_column_stats
      WHERE src_table_name = '{{ table_name }}'
    {% endset %}
    {% set col_results = run_query(col_query) %}
    {% if col_results is not none %}
      {% set column_names = col_results.columns[0].values() %}
      {% set column_list = column_names | join(', ') %}
    {% endif %}
  {% endif %}

  {# Step 3: Build the SQL UNION #}
  {% set sql_parts = [] %}
  {% for db_name in active_dbs %}
    {% set sql %}
      SELECT 
        '{{ db_name }}' AS source_db,
        {{ column_list }}
      FROM [SAPB1_LINKED].{{ db_name }}.dbo.{{ table_name }}
    {% endset %}
    {% do sql_parts.append(sql) %}
  {% endfor %}

  {{ return(sql_parts | join('\nUNION ALL\n')) }}
{% endmacro %}

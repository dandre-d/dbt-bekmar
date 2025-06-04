{% macro sqlserver_proper_case(column_name) %}
    (
        SELECT STRING_AGG(
            UPPER(SUBSTRING(value, 1, 1)) + 
            LOWER(SUBSTRING(value, 2, LEN(value) - 1)), 
            ' '
        ) 
        FROM STRING_SPLIT({{ column_name }}, ' ')
        WHERE value IS NOT NULL
    )
{% endmacro %}

{% macro sqlserver_proper_case_with_exceptions(column_name) %}
    -- Step 1: Replace symbols, normalize input
    REPLACE(
        REPLACE(
            (
                SELECT STRING_AGG(
                    CASE 
                        WHEN UPPER(value) IN ('DJ', 'PTY', '(PTY)','LTD', 'CC', 'NPC', 'LLC') THEN UPPER(value)
                        ELSE UPPER(LEFT(value, 1)) + LOWER(SUBSTRING(value, 2, LEN(value) - 1))
                    END,
                    ' '
                )
                FROM STRING_SPLIT(
                    REPLACE(REPLACE({{ column_name }}, '#', ''), '_', ' '), ' '
                )
                WHERE value IS NOT NULL AND LEN(value) > 0
            ),
        ' .', '.'),
    '  ', ' ')
{% endmacro %}

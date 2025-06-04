{% macro sqlserver_surrogate_key(columns) %}
    LOWER(CONVERT(VARCHAR(64), HASHBYTES(
        'SHA2_256',
        {% if columns | length > 1 %}
            {% for col in columns %}
                ISNULL(CAST({{ col }} AS NVARCHAR), ''){% if not loop.last %} + '|' + {% endif %}
            {% endfor %}
        {% else %}
            -- fallback if only 1 column
            ISNULL(CAST({{ columns[0] }} AS NVARCHAR), '')
        {% endif %}
    ), 2))
{% endmacro %}

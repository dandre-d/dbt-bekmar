{% macro split_camel_case(input_string) %}
  {{ return(input_string
    | regex_replace('([a-z])([A-Z])', '\\1 \\2')
    | regex_replace('([A-Z]+)([A-Z][a-z])', '\\1 \\2')) }}
{% endmacro %}

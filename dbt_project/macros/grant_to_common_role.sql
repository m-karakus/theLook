{% macro grant_to_common_role(model) %}
    {% if execute %}
        grant select on {{ model }} to database role db_role_common
    {% endif %}
{% endmacro %}

{% macro get_demo_duration() %}

    {% set get_max_day %}
        select max(day) from {{ source('complete_journey', 'transaction_data')}}
    {% endset %}

    {% set results = run_query(get_max_day) %}

    {% if execute %}
        {# Return the first column #}
        {% set result = results.columns[0].values()[0] %}
    {% else %}
        {% set result = [] %}
    {% endif %}

    {{ return(result) }}

{% endmacro %}
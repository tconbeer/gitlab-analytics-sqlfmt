{% macro regexp_substr_to_array() %}

    {%- set production_targets = production_targets() -%}
    {%- set db_prep = env_var("SNOWFLAKE_PREP_DATABASE") -%}
    {%- set db_prod = env_var("SNOWFLAKE_PROD_DATABASE") -%}
    {%- set production_databases = [db_prep, db_prod] -%}

    {% for db in production_databases %}
        {%- if target.name in production_targets -%}

            create or replace function
                "{{ db | trim }}".{{ target.schema }}.regexp_to_array(
                    "input_text" string, "regex_text" string
                )

        {%- else -%}

            create or replace function
                "{{ target.database | trim }}_{{ db | trim }}".{{ target.schema }}.regexp_to_array(
                    "input_text" string, "regex_text" string
                )

        {% endif %}
        returns array
        language javascript
        as '

    var regex_constructor = new RegExp(regex_text, "g")
    if (input_text == null) {
        return [];
    }
    matched_substr_array = input_text.match(regex_constructor);
    if (matched_substr_array == null){
        matched_substr_array = []
    }
    return matched_substr_array

'
        ;

    {% endfor %}
{% endmacro %}

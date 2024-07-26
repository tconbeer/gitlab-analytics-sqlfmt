{% macro test_unique_where_currently_valid(model, column_name) %}

    with
        data as (

            select {{ column_name }} as id, count(*) as count_valid_rows
            from {{ model }}
            where is_currently_valid = true
            group by 1

        )

    select count(*)
    from data
    where count_valid_rows != 1

{% endmacro %}

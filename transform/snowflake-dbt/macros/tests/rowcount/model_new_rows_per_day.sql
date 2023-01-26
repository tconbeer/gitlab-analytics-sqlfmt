{% macro model_new_rows_per_day(
    model_name, created_column, min_value, max_value=None, where_clause=None
) %}

with
    dates as (

        select *
        from {{ ref("date_details") }}
        where is_holiday = false and day_of_week in (2, 3, 4, 5, 6)

    ),
    source as (select * from {{ ref(model_name) }}),
    counts as (

        select count(*) as row_count, date_trunc('day', {{ created_column }}) as the_day
        from source
        where
            the_day = current_date - 1 and the_day in (select date_actual from dates)
            {% if where_clause != None %} and {{ where_clause }} {% endif %}
        group by 2
        order by 2 desc
        limit 1

    )

select row_count
from counts
where
    row_count < {{ min_value }}
    {% if max_value != None %} or row_count > {{ max_value }} {% endif %}

{% endmacro %}

{% macro test_no_overlapping_valid_from_to_dates(model, column_name) %}

{% set dates_to_check = (
    "2013-01-01",
    "2016-02-02",
    "2017-03-03",
    "2018-04-04",
    "2019-05-05",
    "2020-06-06",
) %}

with
    data as (select {{ column_name }} as id, valid_from, valid_to from {{ model }}),
    grouped as (

        {% for date in dates_to_check %}
        select id, count(*) as count_rows_valid_on_date
        from data
        inner join
            (select 1) as a
            on '{{ date }}'::date between data.valid_from and coalesce(
                data.valid_to, '9999-12-31'
            )
        group by 1 {{ "UNION" if not loop.last }}
        {% endfor %}

    )

select count(*)
from grouped
where count_rows_valid_on_date != 1


{% endmacro %}

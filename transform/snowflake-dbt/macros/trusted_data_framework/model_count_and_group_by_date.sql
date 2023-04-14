{% macro model_count_and_group_by_date(model_name, date_column_to_group_by) %}

    with
        model_data as (

            select
                cast({{ date_column_to_group_by }} as date) as grouped_date,
                count(*) as num_rows
            from {{ ref(model_name) }}
            group by cast({{ date_column_to_group_by }} as date)

        )

    select distinct db.grouped_date as date_day, ifnull(db.num_rows, 0) as rowcount
    from model_data db

{% endmacro %}

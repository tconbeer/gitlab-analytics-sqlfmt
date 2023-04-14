{% macro source_avg_new_rows_per_month_within_n_stdevs(
    source_name, table, created_column, nr_std_devs=1, where_clause=None
) %}

    with
        source as (select * from {{ source(source_name, table) }}),
        counts as (

            select
                trunc({{ created_column }}, 'Month') as line_created_month,
                count(*) as new_records,
                avg(new_records) over () as average_new_records_per_month,
                stddev(new_records) over () as std_dev_new_records_per_month,
                average_new_records_per_month + (
                    {{ nr_std_devs }} * std_dev_new_records_per_month
                ) as monthly_new_records_threshold
            from source
            {% if where_clause != None %} where {{ where_clause }} {% endif %}
            group by 1
            order by 1

        )

    select new_records
    from counts
    where new_records > monthly_new_records_threshold

{% endmacro %}

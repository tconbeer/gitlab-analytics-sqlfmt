with
    source as (select * from {{ source("engineering", "layout_shift") }}),
    metric_per_row as (

        select
            data_by_row.value['datapoints']::array as datapoints,
            data_by_row.value['target']::varchar as metric_name,
            uploaded_at
        from
            source,
            lateral flatten(input => parse_json(jsontext), outer => true) data_by_row

    ),
    data_points_flushed_out as (

        select
            split_part(metric_name, '.', 13)::varchar as aggregation_name,
            split_part(metric_name, '.', 6)::varchar as metric_name,
            data_by_row.value[0]::float as metric_value,
            data_by_row.value[1]::timestamp as metric_reported_at
        from
            metric_per_row,
            lateral flatten(input => datapoints, outer => true) data_by_row
        where nullif(metric_value::varchar, 'null') is not null
        qualify
            row_number() over (
                partition by metric_name, aggregation_name, metric_reported_at
                order by uploaded_at desc
            ) = 1

    )

select *
from data_points_flushed_out
order by metric_reported_at

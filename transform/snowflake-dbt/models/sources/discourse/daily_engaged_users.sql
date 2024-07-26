with
    source as (select * from {{ source("discourse", "daily_engaged_users") }}),
    parsed as (

        select
            json_value.value['start_date']::datetime as report_start_date,
            json_value.value['title']::varchar as report_title,
            json_value.value['type']::varchar as report_type,
            json_value.value['total']::varchar as report_total,
            data_level_one.value['x']::date as report_value_date,
            data_level_one.value['y']::int as report_value,
            uploaded_at as uploaded_at
        from
            source,
            lateral flatten(input => parse_json(jsontext), outer => true) json_value,
            lateral flatten(json_value.value:data, '') data_level_one

    ),
    dedupe as (

        select distinct
            report_start_date,
            report_title,
            report_type,
            report_value_date,
            report_value,
            max(uploaded_at) as last_uploaded_at
        from parsed {{ dbt_utils.group_by(n=5) }}
    )

select *
from dedupe

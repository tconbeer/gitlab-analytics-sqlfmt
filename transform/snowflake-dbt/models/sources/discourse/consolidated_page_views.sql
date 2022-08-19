with
    source as (select * from {{ source("discourse", "consolidated_page_views") }}),
    parsed as (

        select
            json_value.value['start_date']::datetime as report_start_date,
            json_value.value['title']::varchar as report_title,
            json_value.value['type']::varchar as report_type,
            data_level_one.value['req']::varchar as request_type,
            data_level_one.value['label']::varchar as request_label,
            data_level_two.value['x']::date as report_value_date,
            data_level_two.value['y']::int as report_value,
            uploaded_at as uploaded_at
        from
            source,
            lateral flatten(input => parse_json(jsontext), outer => true) json_value,
            lateral flatten(json_value.value:data, '') data_level_one,
            lateral flatten(data_level_one.value:data, '') data_level_two

    ),
    dedupe as (

        select distinct
            report_start_date,
            report_title,
            report_type,
            request_type,
            request_label,
            report_value_date,
            report_value,
            max(uploaded_at) as last_uploaded_at
        from parsed {{ dbt_utils.group_by(n=7) }}

    )

select *
from dedupe

with
    source as (

        select *
        from {{ source("qualtrics", "survey") }}
        order by uploaded_at desc
        limit 1

    ),
    intermediate as (

        select d.value as data_by_row
        from source, lateral flatten(input => parse_json(jsontext), outer => true) d

    ),
    parsed as (

        select
            data_by_row['id']::varchar as survey_id,
            data_by_row['name']::varchar as survey_name
        from intermediate
        where data_by_row is not null

    )
select *
from parsed

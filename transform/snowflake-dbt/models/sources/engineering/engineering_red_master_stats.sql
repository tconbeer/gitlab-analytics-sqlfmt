with
    source as (

        select *
        from {{ source("engineering", "red_master_stats") }}
        order by uploaded_at desc
        limit 1

    ),
    intermediate as (

        select d.value as data_by_row
        from source, lateral flatten(input => parse_json(jsontext), outer => true) d

    ),
    renamed as (

        select
            data_by_row['date']::date as commit_date,
            data_by_row['id']::varchar as commit_id
        from intermediate

    )

select *
from renamed

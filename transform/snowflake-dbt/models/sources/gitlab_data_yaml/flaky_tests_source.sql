with
    source as (

        select
            *,
            rank() OVER (
                partition by date_trunc('day', uploaded_at) order by uploaded_at desc
            ) as rank
        from {{ source("gitlab_data_yaml", "flaky_tests") }}
        order by uploaded_at desc

    ),
    intermediate as (

        select
            d.value as data_by_row,
            date_trunc('day', uploaded_at)::date as snapshot_date,
            rank
        from source, lateral flatten(input => parse_json(jsontext), outer => true) d

    ),
    renamed as (

        select
            data_by_row['hash']::varchar as hash,
            data_by_row['example_id']::varchar as example_id,
            data_by_row['file']::varchar as file,
            data_by_row['line']::int as line,
            data_by_row['description']::varchar as description,
            data_by_row['last_flaky_job']::varchar as last_flaky_job,
            data_by_row['last_attempts_count']::int as last_attempts_count,
            data_by_row['flaky_reports']::int as flaky_reports,
            data_by_row['first_flaky_at']::timestamp as first_flaky_at,
            data_by_row['last_flaky_at']::timestamp as last_flaky_at,
            snapshot_date,
            rank
        from intermediate

    )

select *
from renamed

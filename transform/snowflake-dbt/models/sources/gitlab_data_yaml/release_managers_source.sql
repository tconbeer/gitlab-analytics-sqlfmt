with
    source as (

        select
            *,
            rank() over (
                partition by date_trunc('day', uploaded_at) order by uploaded_at desc
            ) as rank
        from {{ source("gitlab_data_yaml", "release_managers") }}

    ),
    intermediate as (

        select
            d.value as data_by_row,
            date_trunc('day', uploaded_at)::date as snapshot_date,
            rank
        from source, lateral flatten(input => parse_json(jsontext), outer => true) d

    ),
    intermediate_stage as (

        select
            data_by_row['version']::varchar as major_minor_version,
            split_part(major_minor_version, '.', 1) as major_version,
            split_part(major_minor_version, '.', 2) as minor_version,
            try_to_date(data_by_row['date']::text, 'MMMM DDnd, YYYY') as release_date,
            data_by_row['manager_americas'] [0]::varchar as release_manager_americas,
            data_by_row['manager_apac_emea'] [0]::varchar as release_manager_emea,
            rank,
            snapshot_date
        from intermediate

    )

select *
from intermediate_stage

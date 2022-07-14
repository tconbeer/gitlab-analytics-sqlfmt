with
    source as (

        select
            *,
            rank() OVER (
                partition by date_trunc('day', uploaded_at) order by uploaded_at desc
            ) as rank
        from {{ source("gitlab_data_yaml", "team") }}
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
            data_by_row['departments']::array as departments,
            data_by_row['gitlab']::varchar as gitlab_username,
            data_by_row['name']::varchar as name,
            data_by_row['projects']::varchar as projects,
            data_by_row['slug']::varchar as yaml_slug,
            data_by_row['type']::varchar as type,
            snapshot_date,
            rank
        from intermediate

    )

select *
from renamed

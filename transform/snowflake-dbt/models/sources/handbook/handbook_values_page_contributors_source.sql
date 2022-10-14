with
    contributors as (

        select *
        from {{ source("handbook", "values_before_2020_06") }}

        union all

        select *
        from {{ source("handbook", "values_after_2020_06") }}

    ),
    rename as (

        select
            name::varchar as author_name,
            sha::varchar as git_sha,
            email::varchar as author_email,
            date::timestamp as git_commit_at,
            message::varchar as git_message
        from contributors

    )

select *
from rename

with
    source as (select * from {{ ref("gitlab_dotcom_releases_dedupe_source") }}),
    renamed as (

        select
            id::number as release_id,
            tag::varchar as tag,
            project_id::varchar as project_id,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            author_id::number as author_id
        from source

    )

select *
from renamed

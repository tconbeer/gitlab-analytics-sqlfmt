with
    source as (select * from {{ ref("gitlab_dotcom_snippets_dedupe_source") }}),
    renamed as (

        select
            id::number as snippet_id,
            author_id::number as author_id,
            project_id::number as project_id,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            type::varchar as snippet_type,
            visibility_level::number as visibility_level

        from source

    )

select *
from renamed

with
    source as (select * from {{ ref("gitlab_dotcom_requirements_dedupe_source") }}),
    renamed as (

        select
            id::number as requirement_id,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            project_id::number as project_id,
            author_id::number as author_id,
            iid::number as requirement_iid,
            state::varchar as requirement_state
        from source

    )

select *
from renamed

with
    source as (

        select *
        from {{ ref("gitlab_dotcom_design_management_versions_dedupe_source") }}

    ),
    renamed as (

        select
            id::number as version_id,
            issue_id::number as issue_id,
            created_at::timestamp as created_at,
            author_id::number as author_id
        from source

    )

select *
from renamed

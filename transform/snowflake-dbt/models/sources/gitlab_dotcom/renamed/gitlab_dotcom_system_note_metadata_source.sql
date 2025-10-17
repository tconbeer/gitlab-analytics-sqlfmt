with
    source as (

        select * from {{ ref("gitlab_dotcom_system_note_metadata_dedupe_source") }}

    ),
    renamed as (

        select
            id::number as system_note_metadata_id,
            note_id::number as note_id,
            commit_count::number as commit_count,
            action::varchar as action_type,
            description_version_id::number as description_version_id,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at

        from source

    )

select *
from renamed

with
    source as (

        select *
        from {{ ref("gitlab_dotcom_project_repository_storage_moves_dedupe_source") }}

    ),
    renamed as (

        select
            id::number as project_repository_storage_move_id,
            created_at::timestamp as storage_move_created_at,
            updated_at::timestamp as storage_move_updated_at,
            project_id::number as project_id,
            state::number as state,
            source_storage_name::varchar as source_storage_name,
            destination_storage_name::varchar as destination_storage_name
        from source

    )

select *
from renamed

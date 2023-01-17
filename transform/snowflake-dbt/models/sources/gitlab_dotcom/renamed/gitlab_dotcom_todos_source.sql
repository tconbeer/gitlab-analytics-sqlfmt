with
    source as (select * from {{ ref("gitlab_dotcom_todos_dedupe_source") }}),
    renamed as (

        select
            id::number as todo_id,
            user_id::number as user_id,
            project_id::number as project_id,
            target_id::number as target_id,
            target_type::varchar as target_type,
            author_id::number as author_id,
            action::number as todo_action_id,
            state::varchar as todo_state,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            note_id::number as note_id,
            commit_id::varchar as commit_id,
            group_id::number as group_id

        from source

    )

select *
from renamed

with
    source as (select * from {{ ref("gitlab_dotcom_notes_dedupe_source") }}),
    renamed as (

        select
            id::number as note_id,
            note::varchar as note,
            iff(noteable_type = '', null, noteable_type)::varchar as noteable_type,
            author_id::number as note_author_id,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            project_id::number as project_id,
            attachment::varchar as attachment,
            line_code::varchar as line_code,
            commit_id::varchar as commit_id,
            noteable_id::number as noteable_id,
            system::boolean as system,
            -- st_diff (hidden because not relevant to our current analytics needs)
            updated_by_id::number as note_updated_by_id,
            -- type (hidden because legacy and can be easily confused with
            -- noteable_type)
            position::varchar as position,
            original_position::varchar as original_position,
            resolved_at::timestamp as resolved_at,
            resolved_by_id::number as resolved_by_id,
            discussion_id::varchar as discussion_id,
            cached_markdown_version::number as cached_markdown_version,
            resolved_by_push::boolean as resolved_by_push
        from source

    )

select *
from renamed
where
    note_id not in (
        203215238  -- https://gitlab.com/gitlab-data/analytics/merge_requests/1423
    )

with
    source as (

        select * from {{ ref("gitlab_dotcom_notification_settings_dedupe_source") }}

    ),
    renamed as (

        select

            id::number as notification_settings_id,
            user_id::number as user_id,
            source_id::number as source_id,
            source_type::varchar as source_type,
            level::number as settings_level,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            new_note::boolean as has_new_note_enabled,
            new_issue::boolean as has_new_issue_enabled,
            reopen_issue::boolean as has_reopen_issue_enabled,
            close_issue::boolean as has_close_issue_enabled,
            reassign_issue::boolean as has_reassign_issue_enabled,
            new_merge_request::boolean as has_new_merge_request_enabled,
            reopen_merge_request::boolean as has_reopen_merge_request_enabled,
            close_merge_request::boolean as has_close_merge_request_enabled,
            reassign_merge_request::boolean as has_reassign_merge_request_enabled,
            merge_merge_request::boolean as has_merge_merge_request_enabled,
            failed_pipeline::boolean as has_failed_pipeline_enabled,
            success_pipeline::boolean as has_success_pipeline_enabled

        from source

    )

select *
from renamed

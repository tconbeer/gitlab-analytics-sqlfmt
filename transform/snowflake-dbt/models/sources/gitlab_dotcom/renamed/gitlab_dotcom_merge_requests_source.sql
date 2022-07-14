with
    source as (select * from {{ ref("gitlab_dotcom_merge_requests_dedupe_source") }}),
    renamed as (

        select

            id::number as merge_request_id,
            iid::number as merge_request_iid,
            title::varchar as merge_request_title,

            iff(lower(target_branch) = 'master', true, false) as is_merge_to_master,
            iff(lower(merge_error) = 'nan', null, merge_error) as merge_error,
            assignee_id::number as assignee_id,
            updated_by_id::number as updated_by_id,
            merge_user_id::number as merge_user_id,
            last_edited_by_id::number as last_edited_by_id,
            milestone_id::number as milestone_id,
            head_pipeline_id::number as head_pipeline_id,
            latest_merge_request_diff_id::number as latest_merge_request_diff_id,
            approvals_before_merge::number as approvals_before_merge,
            lock_version::number as lock_version,
            time_estimate::number as time_estimate,
            source_project_id::number as project_id,
            target_project_id::number as target_project_id,
            author_id::number as author_id,
            state_id::number as merge_request_state_id,
            -- Override state by mapping state_id. See issue #3556.
            {{ map_state_id("state_id") }} as merge_request_state,
            merge_status as merge_request_status,
            merge_when_pipeline_succeeds::boolean as does_merge_when_pipeline_succeeds,
            squash::boolean as does_squash,
            discussion_locked::boolean as is_discussion_locked,
            allow_maintainer_to_push::boolean as does_allow_maintainer_to_push,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            last_edited_at::timestamp as merge_request_last_edited_at,
            description::varchar as merge_request_description

        -- merge_params // hidden for privacy
        from source

    )

select *
from renamed

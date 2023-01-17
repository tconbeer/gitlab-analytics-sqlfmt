with
    source as (select * from {{ ref("gitlab_dotcom_projects_dedupe_source") }}),
    renamed as (

        select

            id::number as project_id,
            description::varchar as project_description,
            import_source::varchar as project_import_source,
            issues_template::varchar as project_issues_template,
            build_coverage_regex as project_build_coverage_regex,
            name::varchar as project_name,
            path::varchar as project_path,
            import_url::varchar as project_import_url,
            merge_requests_template as project_merge_requests_template,

            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,

            creator_id::number as creator_id,
            namespace_id::number as namespace_id,

            last_activity_at::timestamp as last_activity_at,

            case
                when visibility_level = '20'
                then 'public'
                when visibility_level = '10'
                then 'internal'
                else 'private'
            end::varchar as visibility_level,

            archived::boolean as archived,

            iff(avatar is null, false, true)::boolean as has_avatar,

            star_count::number as project_star_count,
            merge_requests_rebase_enabled::boolean as merge_requests_rebase_enabled,
            iff(lower(import_type) = 'nan', null, import_type) as import_type,
            approvals_before_merge::number as approvals_before_merge,
            reset_approvals_on_push::boolean as reset_approvals_on_push,
            merge_requests_ff_only_enabled::boolean as merge_requests_ff_only_enabled,
            mirror::boolean as mirror,
            mirror_user_id::number as mirror_user_id,
            shared_runners_enabled::boolean as shared_runners_enabled,
            build_allow_git_fetch::boolean as build_allow_git_fetch,
            build_timeout::number as build_timeout,
            mirror_trigger_builds::boolean as mirror_trigger_builds,
            pending_delete::boolean as pending_delete,
            public_builds::boolean as public_builds,
            last_repository_check_failed::boolean as last_repository_check_failed,
            last_repository_check_at::timestamp as last_repository_check_at,
            container_registry_enabled::boolean as container_registry_enabled,
            only_allow_merge_if_pipeline_succeeds::boolean
            as only_allow_merge_if_pipeline_succeeds,
            has_external_issue_tracker::boolean as has_external_issue_tracker,
            repository_storage,
            repository_read_only::boolean as repository_read_only,
            request_access_enabled::boolean as request_access_enabled,
            has_external_wiki::boolean as has_external_wiki,
            ci_config_path,
            lfs_enabled::boolean as lfs_enabled,
            only_allow_merge_if_all_discussions_are_resolved::boolean
            as only_allow_merge_if_all_discussions_are_resolved,
            repository_size_limit::number as repository_size_limit,
            printing_merge_request_link_enabled::boolean
            as printing_merge_request_link_enabled,
            iff(
                auto_cancel_pending_pipelines::int = 1, true, false
            ) as has_auto_canceling_pending_pipelines,
            service_desk_enabled::boolean as service_desk_enabled,
            iff(lower(delete_error) = 'nan', null, delete_error) as delete_error,
            last_repository_updated_at::timestamp as last_repository_updated_at,
            storage_version::number as storage_version,
            resolve_outdated_diff_discussions::boolean
            as resolve_outdated_diff_discussions,
            disable_overriding_approvers_per_merge_request::boolean
            as disable_overriding_approvers_per_merge_request,
            remote_mirror_available_overridden::boolean
            as remote_mirror_available_overridden,
            only_mirror_protected_branches::boolean as only_mirror_protected_branches,
            pull_mirror_available_overridden::boolean
            as pull_mirror_available_overridden,
            mirror_overwrites_diverged_branches::boolean
            as mirror_overwrites_diverged_branches,
            external_authorization_classification_label
        from source

    )

select *
from renamed

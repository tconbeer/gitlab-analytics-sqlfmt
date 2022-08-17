{% set sensitive_fields = [
    "project_description",
    "project_import_source",
    "project_issues_template",
    "project_build_coverage_regex",
    "project_name",
    "project_path",
    "project_import_url",
    "project_merge_requests_template",
] %}

with
    projects as (select * from {{ ref("gitlab_dotcom_projects") }}),

    namespaces as (select * from {{ ref("gitlab_dotcom_namespaces") }}),

    members as (

        select *
        from {{ ref("gitlab_dotcom_members") }} members
        where
            is_currently_valid = true
            and {{ filter_out_blocked_users("members", "user_id") }}

    ),

    namespace_lineage as (select * from {{ ref("gitlab_dotcom_namespace_lineage") }}),

    gitlab_subscriptions as (

        select *
        from {{ ref("gitlab_dotcom_gitlab_subscriptions_snapshots_namespace_id_base") }}

    ),

    active_services as (

        select * from {{ ref("gitlab_dotcom_services") }} where is_active = true

    ),

    joined as (
        select
            projects.project_id,
            projects.created_at,  -- We will phase out `project_created_at`
            projects.created_at as project_created_at,
            projects.updated_at as project_updated_at,
            projects.creator_id,
            projects.namespace_id,
            projects.last_activity_at,
            projects.visibility_level,
            projects.archived,
            projects.has_avatar,
            projects.project_star_count,
            projects.merge_requests_rebase_enabled,
            projects.import_type,
            projects.approvals_before_merge,
            projects.reset_approvals_on_push,
            projects.merge_requests_ff_only_enabled,
            projects.mirror,
            projects.mirror_user_id,
            projects.shared_runners_enabled,
            projects.build_allow_git_fetch,
            projects.build_timeout,
            projects.mirror_trigger_builds,
            projects.pending_delete,
            projects.public_builds,
            projects.last_repository_check_failed,
            projects.last_repository_check_at,
            projects.container_registry_enabled,
            projects.only_allow_merge_if_pipeline_succeeds,
            projects.has_external_issue_tracker,
            projects.repository_storage,
            projects.repository_read_only,
            projects.request_access_enabled,
            projects.has_external_wiki,
            projects.ci_config_path,
            projects.lfs_enabled,
            projects.only_allow_merge_if_all_discussions_are_resolved,
            projects.repository_size_limit,
            projects.printing_merge_request_link_enabled,
            projects.has_auto_canceling_pending_pipelines,
            projects.service_desk_enabled,
            projects.delete_error,
            projects.last_repository_updated_at,
            projects.storage_version,
            projects.resolve_outdated_diff_discussions,
            projects.disable_overriding_approvers_per_merge_request,
            projects.remote_mirror_available_overridden,
            projects.only_mirror_protected_branches,
            projects.pull_mirror_available_overridden,
            projects.mirror_overwrites_diverged_branches,
            iff(
                projects.import_type = 'gitlab_project'
                and projects.project_path = 'learn-gitlab',
                true,
                false
            ) as is_learn_gitlab,

            {% for field in sensitive_fields %}
            case
                when
                    projects.visibility_level != 'public'
                    and not namespace_lineage.namespace_is_internal
                then 'project is private/internal'
                else {{ field }}
            end as {{ field }},
            {% endfor %}

            namespaces.namespace_name,
            namespaces.namespace_path,

            namespace_lineage.namespace_is_internal,
            namespace_lineage.namespace_plan_id,
            namespace_lineage.namespace_plan_title,
            namespace_lineage.namespace_plan_is_paid,
            namespace_lineage.ultimate_parent_id,
            namespace_lineage.ultimate_parent_plan_id,
            namespace_lineage.ultimate_parent_plan_title,
            namespace_lineage.ultimate_parent_plan_is_paid,

            case
                when gitlab_subscriptions.is_trial
                then 'trial'
                else coalesce(gitlab_subscriptions.plan_id, 34)::varchar
            end as plan_id_at_project_creation,
            case
                when import_type is null
                then null
                when import_type = 'gitlab_project' and project_import_url is null
                then 'project_template'
                when import_type = 'gitlab_project' and project_import_url is not null
                then 'gitlab_project_import'
                when
                    import_type is not null
                    and import_type != 'gitlab_project'
                    and project_import_url is not null
                then 'other_source_project_import'
            end as project_template,
            arrayagg(active_services.service_type) as active_service_types,
            coalesce(count(distinct members.member_id), 0) as member_count
        from projects
        left join
            members
            on projects.project_id = members.source_id
            and members.member_source_type = 'Project'
        left join namespaces on projects.namespace_id = namespaces.namespace_id
        left join
            namespace_lineage
            on namespaces.namespace_id = namespace_lineage.namespace_id
        left join
            gitlab_subscriptions
            on namespace_lineage.ultimate_parent_id = gitlab_subscriptions.namespace_id
            and projects.created_at
            between gitlab_subscriptions.valid_from
            and {{ coalesce_to_infinity("gitlab_subscriptions.valid_to") }}
        left join
            active_services on projects.project_id = active_services.project_id
            {{ dbt_utils.group_by(n=70) }}
    )

select *
from joined

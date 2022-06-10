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

{{
    simple_cte(
        [
            ("dim_date", "dim_date"),
            (
                "gitlab_subscriptions",
                "gitlab_dotcom_gitlab_subscriptions_snapshots_namespace_id_base",
            ),
            ("members_source", "gitlab_dotcom_members_source"),
            (
                "namespace_lineage_historical",
                "gitlab_dotcom_namespace_lineage_historical_daily",
            ),
            ("plans", "gitlab_dotcom_plans_source"),
            ("prep_namespace", "prep_namespace"),
            ("projects_source", "gitlab_dotcom_projects_source"),
            ("prep_product_tier", "prep_product_tier"),
        ]
    )
}}

,
active_services as (

    select * from {{ ref("gitlab_dotcom_services_source") }} where is_active = true

),
namespace_lineage as (

    select
        namespace_lineage_historical.*,
        iff(
            row_number() over (
                partition by namespace_lineage_historical.namespace_id
                order by namespace_lineage_historical.snapshot_day desc
            ) = 1,
            true,
            false
        ) as is_current,
        namespace_lineage_historical.snapshot_day
        = current_date
        as ultimate_parent_is_current,
        plans.plan_title as ultimate_parent_plan_title,
        plans.plan_is_paid as ultimate_parent_plan_is_paid,
        plans.plan_name as ultimate_parent_plan_name
    from namespace_lineage_historical
    inner join
        plans on namespace_lineage_historical.ultimate_parent_plan_id = plans.plan_id
    qualify
        row_number() over (
            partition by
                namespace_lineage_historical.namespace_id,
                namespace_lineage_historical.parent_id,
                namespace_lineage_historical.ultimate_parent_id
            order by namespace_lineage_historical.snapshot_day desc
        ) = 1

),
joined as (

    select
        projects_source.project_id as dim_project_id,
        projects_source.namespace_id as dim_namespace_id,
        namespace_lineage.ultimate_parent_id as ultimate_parent_namespace_id,
        projects_source.creator_id as dim_user_id_creator,
        dim_date.date_id as dim_date_id,

        -- plan/product tier metadata at creation
        prep_namespace.dim_product_tier_id as dim_product_tier_id_at_creation,
        prep_namespace.gitlab_plan_id as dim_plan_id,
        -- projects metadata
        projects_source.created_at as created_at,
        projects_source.updated_at as updated_at,
        projects_source.last_activity_at,
        projects_source.visibility_level,
        projects_source.archived as is_archived,
        projects_source.has_avatar,
        projects_source.project_star_count,
        projects_source.merge_requests_rebase_enabled,
        projects_source.import_type,
        iff(projects_source.import_type is not null, true, false) as is_imported,
        projects_source.approvals_before_merge,
        projects_source.reset_approvals_on_push,
        projects_source.merge_requests_ff_only_enabled,
        projects_source.mirror,
        projects_source.mirror_user_id,
        projects_source.shared_runners_enabled,
        projects_source.build_allow_git_fetch,
        projects_source.build_timeout,
        projects_source.mirror_trigger_builds,
        projects_source.pending_delete,
        projects_source.public_builds,
        projects_source.last_repository_check_failed,
        projects_source.last_repository_check_at,
        projects_source.container_registry_enabled,
        projects_source.only_allow_merge_if_pipeline_succeeds,
        projects_source.has_external_issue_tracker,
        projects_source.repository_storage,
        projects_source.repository_read_only,
        projects_source.request_access_enabled,
        projects_source.has_external_wiki,
        projects_source.ci_config_path,
        projects_source.lfs_enabled,
        projects_source.only_allow_merge_if_all_discussions_are_resolved,
        projects_source.repository_size_limit,
        projects_source.printing_merge_request_link_enabled,
        projects_source.has_auto_canceling_pending_pipelines,
        projects_source.service_desk_enabled,
        projects_source.delete_error,
        projects_source.last_repository_updated_at,
        projects_source.storage_version,
        projects_source.resolve_outdated_diff_discussions,
        projects_source.disable_overriding_approvers_per_merge_request,
        projects_source.remote_mirror_available_overridden,
        projects_source.only_mirror_protected_branches,
        projects_source.pull_mirror_available_overridden,
        projects_source.mirror_overwrites_diverged_branches,
        -- namespace metadata
        ifnull(prep_namespace.namespace_is_internal, false) as namespace_is_internal,

        {% for field in sensitive_fields %}
        case
            when
                projects_source.visibility_level != 'public'
                and not namespace_lineage.namespace_is_internal
            then 'project is private/internal'
            else {{ field }}
        end as {{ field }},
        {% endfor %}
        iff(
            projects_source.import_type = 'gitlab_project'
            and projects_source.project_path = 'learn-gitlab',
            true,
            false
        ) as is_learn_gitlab,
        arrayagg(active_services.service_type) as active_service_types_array,

        ifnull(count(distinct members_source.member_id), 0) as member_count
    from projects_source
    left join dim_date on to_date(projects_source.created_at) = dim_date.date_day
    left join
        prep_namespace
        on projects_source.namespace_id = prep_namespace.dim_namespace_id
        and prep_namespace.is_currently_valid
    left join
        members_source
        on projects_source.project_id = members_source.source_id
        and members_source.member_source_type = 'Project'
    left join
        namespace_lineage
        on prep_namespace.dim_namespace_id = namespace_lineage.namespace_id
        and namespace_lineage.is_current = true
    left join
        gitlab_subscriptions
        on namespace_lineage.ultimate_parent_id = gitlab_subscriptions.namespace_id
        and projects_source.created_at >= gitlab_subscriptions.valid_from
        and projects_source.created_at
        < {{ coalesce_to_infinity("gitlab_subscriptions.valid_to") }}
    left join
        active_services on projects_source.project_id = active_services.project_id
        {{ dbt_utils.group_by(n=63) }}

)

select *
from joined

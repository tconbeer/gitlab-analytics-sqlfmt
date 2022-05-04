{{ config(tags=["product"]) }}

{% set year_value = var(
    "year", (run_started_at - modules.datetime.timedelta(2)).strftime("%Y")
) %}
{% set month_value = var(
    "month", (run_started_at - modules.datetime.timedelta(2)).strftime("%m")
) %}


{%- set event_ctes = [
    {
        "event_name": "action",
        "source_cte_name": "prep_action",
        "user_column_name": "dim_user_id",
        "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
        "project_column_name": "dim_project_id",
        "primary_key": "dim_action_id",
        "stage_name": "manage",
    },
    {
        "event_name": "dast_build_run",
        "source_cte_name": "dast_jobs",
        "user_column_name": "dim_user_id",
        "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
        "project_column_name": "dim_project_id",
        "primary_key": "dim_ci_build_id",
        "stage_name": "secure",
    },
    {
        "event_name": "dependency_scanning_build_run",
        "source_cte_name": "dependency_scanning_jobs",
        "user_column_name": "dim_user_id",
        "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
        "project_column_name": "dim_project_id",
        "primary_key": "dim_ci_build_id",
        "stage_name": "secure",
    },
    {
        "event_name": "deployment_creation",
        "source_cte_name": "prep_deployment",
        "user_column_name": "dim_user_id",
        "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
        "project_column_name": "dim_project_id",
        "primary_key": "dim_deployment_id",
        "stage_name": "release",
    },
    {
        "event_name": "epic_creation",
        "source_cte_name": "prep_epic",
        "user_column_name": "author_id",
        "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
        "project_column_name": "NULL",
        "primary_key": "dim_epic_id",
        "stage_name": "plan",
    },
    {
        "event_name": "issue_creation",
        "source_cte_name": "prep_issue",
        "user_column_name": "author_id",
        "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
        "project_column_name": "dim_project_id",
        "primary_key": "dim_issue_id",
        "stage_name": "plan",
    },
    {
        "event_name": "issue_note_creation",
        "source_cte_name": "issue_note",
        "user_column_name": "author_id",
        "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
        "project_column_name": "dim_project_id",
        "primary_key": "dim_note_id",
        "stage_name": "plan",
    },
    {
        "event_name": "license_scanning_build_run",
        "source_cte_name": "license_scanning_jobs",
        "user_column_name": "dim_user_id",
        "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
        "project_column_name": "dim_project_id",
        "primary_key": "dim_ci_build_id",
        "stage_name": "secure",
    },
    {
        "event_name": "merge_request_creation",
        "source_cte_name": "prep_merge_request",
        "user_column_name": "author_id",
        "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
        "project_column_name": "dim_project_id",
        "primary_key": "dim_merge_request_id",
        "stage_name": "create",
    },
    {
        "event_name": "merge_request_note_creation",
        "source_cte_name": "merge_request_note",
        "user_column_name": "author_id",
        "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
        "project_column_name": "dim_project_id",
        "primary_key": "dim_note_id",
        "stage_name": "create",
    },
    {
        "event_name": "ci_pipeline_creation",
        "source_cte_name": "prep_ci_pipeline",
        "user_column_name": "dim_user_id",
        "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
        "project_column_name": "dim_project_id",
        "primary_key": "dim_ci_pipeline_id",
        "stage_name": "verify",
    },
    {
        "event_name": "package_creation",
        "source_cte_name": "prep_package",
        "user_column_name": "creator_id",
        "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
        "project_column_name": "dim_project_id",
        "primary_key": "dim_package_id",
        "stage_name": "package",
    },
    {
        "event_name": "protect_ci_build_creation",
        "source_cte_name": "protect_ci_build",
        "user_column_name": "dim_user_id",
        "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
        "project_column_name": "dim_project_id",
        "primary_key": "dim_ci_build_id",
        "stage_name": "protect",
    },
    {
        "event_name": "push_action",
        "source_cte_name": "push_actions",
        "user_column_name": "dim_user_id",
        "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
        "project_column_name": "dim_project_id",
        "primary_key": "dim_action_id",
        "stage_name": "create",
    },
    {
        "event_name": "release_creation",
        "source_cte_name": "prep_release",
        "user_column_name": "author_id",
        "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
        "project_column_name": "dim_project_id",
        "primary_key": "dim_release_id",
        "stage_name": "release",
    },
    {
        "event_name": "requirement_creation",
        "source_cte_name": "prep_requirement",
        "user_column_name": "author_id",
        "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
        "project_column_name": "dim_project_id",
        "primary_key": "dim_requirement_id",
        "stage_name": "plan",
    },
    {
        "event_name": "sast_build_run",
        "source_cte_name": "sast_jobs",
        "user_column_name": "dim_user_id",
        "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
        "project_column_name": "dim_project_id",
        "primary_key": "dim_ci_build_id",
        "stage_name": "secure",
    },
    {
        "event_name": "secret_detection_build_run",
        "source_cte_name": "secret_detection_jobs",
        "user_column_name": "dim_user_id",
        "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
        "project_column_name": "dim_project_id",
        "primary_key": "dim_ci_build_id",
        "stage_name": "secure",
    },
    {
        "event_name": "other_ci_build_creation",
        "source_cte_name": "other_ci_build",
        "user_column_name": "dim_user_id",
        "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
        "project_column_name": "dim_project_id",
        "primary_key": "dim_ci_build_id",
        "stage_name": "verify",
    },
    {
        "event_name": "successful_ci_pipeline_creation",
        "source_cte_name": "successful_ci_pipelines",
        "user_column_name": "dim_user_id",
        "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
        "project_column_name": "dim_project_id",
        "primary_key": "dim_ci_pipeline_id",
        "stage_name": "verify",
    },
    {
        "event_name": "action_monthly_active_users_project_repo",
        "source_cte_name": "monthly_active_users_project_repo",
        "user_column_name": "dim_user_id",
        "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
        "project_column_name": "dim_project_id",
        "primary_key": "dim_action_id",
        "stage_name": "create",
    },
    {
        "event_name": "ci_stages",
        "source_cte_name": "prep_ci_stage",
        "user_column_name": "NULL",
        "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
        "project_column_name": "dim_project_id",
        "primary_key": "dim_ci_stage_id",
        "stage_name": "configure",
    },
    {
        "event_name": "notes",
        "source_cte_name": "prep_note",
        "user_column_name": "author_id",
        "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
        "project_column_name": "dim_project_id",
        "primary_key": "dim_note_id",
        "stage_name": "plan",
    },
    {
        "event_name": "todos",
        "source_cte_name": "prep_todo",
        "user_column_name": "author_id",
        "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
        "project_column_name": "dim_project_id",
        "primary_key": "dim_todo_id",
        "stage_name": "plan",
    },
    {
        "event_name": "issue_resource_label_events",
        "source_cte_name": "issue_resource_label_events",
        "user_column_name": "dim_user_id",
        "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
        "project_column_name": "dim_project_id",
        "primary_key": "dim_issue_label_id",
        "stage_name": "plan",
    },
    {
        "event_name": "environments",
        "source_cte_name": "prep_environment_event",
        "user_column_name": "NULL",
        "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
        "project_column_name": "dim_project_id",
        "primary_key": "dim_environment_id",
        "stage_name": "release",
    },
    {
        "event_name": "issue_resource_milestone_events",
        "source_cte_name": "issue_resource_milestone",
        "user_column_name": "dim_user_id",
        "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
        "project_column_name": "dim_project_id",
        "primary_key": "dim_resource_milestone_id",
        "stage_name": "plan",
    },
    {
        "event_name": "labels",
        "source_cte_name": "prep_labels",
        "user_column_name": "NULL",
        "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
        "project_column_name": "dim_project_id",
        "primary_key": "dim_label_id",
        "stage_name": "plan",
    },
    {
        "event_name": "terraform_reports",
        "source_cte_name": "terraform_reports_events",
        "user_column_name": "NULL",
        "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
        "project_column_name": "dim_project_id",
        "primary_key": "dim_ci_job_artifact_id",
        "stage_name": "configure",
    },
    {
        "event_name": "users_created",
        "source_cte_name": "prep_user_event",
        "user_column_name": "dim_user_id",
        "ultimate_parent_namespace_column_name": "NULL",
        "project_column_name": "NULL",
        "primary_key": "dim_user_id",
        "stage_name": "manage",
    },
    {
        "event_name": "action_monthly_active_users_wiki_repo",
        "source_cte_name": "action_monthly_active_users_wiki_repo_source",
        "user_column_name": "dim_user_id",
        "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
        "project_column_name": "dim_project_id",
        "primary_key": "dim_action_id",
        "stage_name": "create",
    },
    {
        "event_name": "epic_notes",
        "source_cte_name": "epic_notes_source",
        "user_column_name": "author_id",
        "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
        "project_column_name": "NULL",
        "primary_key": "dim_note_id",
        "stage_name": "plan",
    },
    {
        "event_name": "boards",
        "source_cte_name": "prep_board",
        "user_column_name": "NULL",
        "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
        "project_column_name": "dim_project_id",
        "primary_key": "dim_board_id",
        "stage_name": "plan",
    },
    {
        "event_name": "project_auto_devops",
        "source_cte_name": "prep_project_auto_devops",
        "user_column_name": "NULL",
        "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
        "project_column_name": "dim_project_id",
        "primary_key": "dim_auto_devops_id",
        "stage_name": "configure",
    },
    {
        "event_name": "services",
        "source_cte_name": "prep_service",
        "user_column_name": "NULL",
        "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
        "project_column_name": "dim_project_id",
        "primary_key": "dim_service_id",
        "stage_name": "create",
    },
    {
        "event_name": "issue_resource_weight_events",
        "source_cte_name": "prep_issue_resource_weight",
        "user_column_name": "dim_user_id",
        "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
        "project_column_name": "dim_project_id",
        "primary_key": "dim_resource_weight_id",
        "stage_name": "plan",
    },
    {
        "event_name": "milestones",
        "source_cte_name": "prep_milestone",
        "user_column_name": "NULL",
        "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
        "project_column_name": "dim_project_id",
        "primary_key": "dim_milestone_id",
        "stage_name": "plan",
    },
    {
        "event_name": "action_monthly_active_users_design_management",
        "source_cte_name": "action_monthly_active_users_design_management_source",
        "user_column_name": "dim_user_id",
        "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
        "project_column_name": "dim_project_id",
        "primary_key": "dim_action_id",
        "stage_name": "create",
    },
    {
        "event_name": "ci_pipeline_schedules",
        "source_cte_name": "prep_ci_pipeline_schedule",
        "user_column_name": "dim_user_id",
        "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
        "project_column_name": "dim_project_id",
        "primary_key": "dim_ci_pipeline_schedule_id",
        "stage_name": "verify",
    },
    {
        "event_name": "snippets",
        "source_cte_name": "prep_snippet",
        "user_column_name": "author_id",
        "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
        "project_column_name": "dim_project_id",
        "primary_key": "dim_snippet_id",
        "stage_name": "create",
    },
    {
        "event_name": "projects_prometheus_active",
        "source_cte_name": "project_prometheus_source",
        "user_column_name": "dim_user_id_creator",
        "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
        "project_column_name": "dim_project_id",
        "primary_key": "dim_project_id",
        "stage_name": "monitor",
    },
    {
        "event_name": "ci_triggers",
        "source_cte_name": "prep_ci_trigger",
        "user_column_name": "owner_id",
        "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
        "project_column_name": "dim_project_id",
        "primary_key": "dim_ci_trigger_id",
        "stage_name": "verify",
    },
    {
        "event_name": "incident_labeled_issues",
        "source_cte_name": "incident_labeled_issues_source",
        "user_column_name": "author_id",
        "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
        "project_column_name": "dim_project_id",
        "primary_key": "dim_issue_id",
        "stage_name": "monitor",
    },
    {
        "event_name": "api_fuzzing_build_run",
        "source_cte_name": "api_fuzzing_jobs",
        "user_column_name": "dim_user_id",
        "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
        "project_column_name": "dim_project_id",
        "primary_key": "dim_ci_build_id",
        "stage_name": "secure",
    },
] -%}

{{
    simple_cte(
        [
            ("prep_ci_pipeline", "prep_ci_pipeline"),
            ("prep_action", "prep_action"),
            ("prep_ci_build", "prep_ci_build"),
            ("prep_deployment", "prep_deployment"),
            ("prep_epic", "prep_epic"),
            ("prep_issue", "prep_issue"),
            ("prep_merge_request", "prep_merge_request"),
            ("prep_note", "prep_note"),
            ("prep_package", "prep_package"),
            ("prep_release", "prep_release"),
            ("prep_requirement", "prep_requirement"),
            ("dim_project", "dim_project"),
            ("prep_namespace", "prep_namespace"),
            ("prep_user", "prep_user"),
            ("prep_plan", "prep_gitlab_dotcom_plan"),
            ("prep_namespace_plan_hist", "prep_namespace_plan_hist"),
            ("prep_ci_stage", "prep_ci_stage"),
            ("prep_note", "prep_note"),
            ("prep_todo", "prep_todo"),
            ("prep_resource_label", "prep_resource_label"),
            ("prep_environment_event", "prep_environment_event"),
            ("prep_resource_milestone", "prep_resource_milestone"),
            ("prep_labels", "prep_labels"),
            ("prep_ci_artifact", "prep_ci_artifact"),
            ("prep_user_event", "prep_user"),
            ("prep_board", "prep_board"),
            ("prep_project_auto_devops", "prep_project_auto_devops"),
            ("prep_service", "prep_service"),
            ("prep_issue_resource_weight", "prep_issue_resource_weight"),
            ("prep_milestone", "prep_milestone"),
            ("prep_ci_pipeline_schedule", "prep_ci_pipeline_schedule"),
            ("prep_snippet", "prep_snippet"),
            ("prep_project", "prep_project"),
            ("prep_ci_trigger", "prep_ci_trigger"),
        ]
    )
}}

,
dast_jobs as (select * from prep_ci_build where secure_ci_build_type = 'dast'),
dependency_scanning_jobs as (

    select * from prep_ci_build where secure_ci_build_type = 'dependency_scanning'

),
push_actions as (select * from prep_action where event_action_type = 'pushed'),
issue_note as (select * from prep_note where noteable_type = 'Issue'),
license_scanning_jobs as (

    select *
    from prep_ci_build
    where secure_ci_build_type in ('license_scanning', 'license_management')

),
merge_request_note as (select * from prep_note where noteable_type = 'MergeRequest'),
protect_ci_build as (

    select * from prep_ci_build where secure_ci_build_type = 'container_scanning'

),
sast_jobs as (select * from prep_ci_build where secure_ci_build_type = 'sast'),
secret_detection_jobs as (

    select * from prep_ci_build where secure_ci_build_type = 'secret_detection'

),
other_ci_build as (select * from prep_ci_build where secure_ci_build_type is null),
api_fuzzing_jobs as (

    select * from prep_ci_build where secure_ci_build_type = 'api_fuzzing'

),
successful_ci_pipelines as (

    select * from prep_ci_pipeline where failure_reason is null

),
monthly_active_users_project_repo as (

    select * from prep_action where target_type is null and event_action_type = 'pushed'

),
issue_resource_label_events as (

    select * from prep_resource_label where dim_issue_id is not null

),
issue_resource_milestone as (

    select * from prep_resource_milestone where issue_id is not null

),
terraform_reports_events as (select * from prep_ci_artifact where file_type = 18),
action_monthly_active_users_wiki_repo_source as (

    select *
    from prep_action
    where target_type = 'WikiPage::Meta' and event_action_type in ('created', 'updated')

),
epic_notes_source as (select * from prep_note where noteable_type = 'Epic'),
action_monthly_active_users_design_management_source as (

    select *
    from prep_action
    where
        target_type = 'DesignManagement::Design' and event_action_type in (
            'created', 'updated'
        )

),
project_prometheus_source as (

    select *, dim_date_id as created_date_id
    from prep_project
    where array_contains('PrometheusService'::variant, active_service_types_array)

),
incident_labeled_issues_source as (

    select * from prep_issue where array_contains('incident'::variant, labels)

),
data as (

    {% for event_cte in event_ctes %}

    select
        md5(
            {{ event_cte.source_cte_name }}.{{ event_cte.primary_key }}
            || '-'
            || '{{ event_cte.event_name }}'
        ) as event_id,
        '{{ event_cte.event_name }}' as event_name,
        '{{ event_cte.stage_name }}' as stage_name,
        {{ event_cte.source_cte_name }}.created_at as event_created_at,
        {{ event_cte.source_cte_name }}.created_date_id as created_date_id,
        {%- if event_cte.project_column_name != "NULL" %}
        {{ event_cte.source_cte_name }}.{{ event_cte.project_column_name }}
        as dim_project_id,
        'project' as parent_type,
        {{ event_cte.source_cte_name }}.{{ event_cte.project_column_name }}
        as parent_id,
        {{ event_cte.source_cte_name }}.ultimate_parent_namespace_id
        as ultimate_parent_namespace_id,
        {%- elif event_cte.ultimate_parent_namespace_column_name != "NULL" %}
        null as dim_project_id,
        'group' as parent_type,
        {{ event_cte.source_cte_name }}.{{ event_cte.ultimate_parent_namespace_column_name }}
        as parent_id,
        {{ event_cte.source_cte_name }}.ultimate_parent_namespace_id
        as ultimate_parent_namespace_id,
        {%- else %}
        null as dim_project_id,
        null as parent_type,
        null as parent_id,
        null as ultimate_parent_namespace_id,
        {%- endif %}
        {%- if event_cte.project_column_name != "NULL" or event_cte.ultimate_parent_namespace_column_name != "NULL" %}
        coalesce(
            {{ event_cte.source_cte_name }}.dim_plan_id, 34
        ) as plan_id_at_event_date,
        coalesce(prep_plan.plan_name, 'free') as plan_name_at_event_date,
        coalesce(prep_plan.plan_is_paid, false) as plan_was_paid_at_event_date,
        {%- else %}
        34 as plan_id_at_event_date,
        'free' as plan_name_at_event_date,
        false as plan_was_paid_at_event_date,
        {%- endif %}
        {%- if event_cte.user_column_name != "NULL" %}
        {{ event_cte.source_cte_name }}.{{ event_cte.user_column_name }} as dim_user_id,
        prep_user.created_at as user_created_at,
        to_date(prep_user.created_at) as user_created_date,
        floor(
            datediff(
                'day',
                prep_user.created_at::date,
                {{ event_cte.source_cte_name }}.created_at::date
            )
        ) as days_since_user_creation_at_event_date,
        {%- else %}
        null as dim_user_id,
        null as user_created_at,
        null as user_created_date,
        null as days_since_user_creation_at_event_date,
        {%- endif %}
        {%- if event_cte.ultimate_parent_namespace_column_name != "NULL" %}
        prep_namespace.created_at as namespace_created_at,
        to_date(prep_namespace.created_at) as namespace_created_date,
        ifnull(blocked_user.is_blocked_user, false) as is_blocked_namespace_creator,
        prep_namespace.namespace_is_internal as namespace_is_internal,
        floor(
            datediff(
                'day',
                prep_namespace.created_at::date,
                {{ event_cte.source_cte_name }}.created_at::date
            )
        ) as days_since_namespace_creation_at_event_date,
        {%- else %}
        null as namespace_created_at,
        null as namespace_created_date,
        null as is_blocked_namespace_creator,
        null as namespace_is_internal,
        null as days_since_namespace_creation_at_event_date,
        {%- endif %}
        {%- if event_cte.project_column_name != "NULL" %}
        floor(
            datediff(
                'day',
                dim_project.created_at::date,
                {{ event_cte.source_cte_name }}.created_at::date
            )
        ) as days_since_project_creation_at_event_date,
        ifnull(dim_project.is_imported, false) as project_is_imported,
        dim_project.is_learn_gitlab as project_is_learn_gitlab
        {%- else %}
        null as days_since_project_creation_at_event_date,
        null as project_is_imported,
        null as project_is_learn_gitlab
        {%- endif %}
    from {{ event_cte.source_cte_name }}
    {%- if event_cte.project_column_name != "NULL" %}
    left join
        dim_project
        on {{ event_cte.source_cte_name }}.{{ event_cte.project_column_name }}
        = dim_project.dim_project_id
    {%- endif %}
    {%- if event_cte.ultimate_parent_namespace_column_name != "NULL" %}
    left join
        prep_namespace
        on {{ event_cte.source_cte_name }}.{{ event_cte.ultimate_parent_namespace_column_name }}
        = prep_namespace.dim_namespace_id
        and prep_namespace.is_currently_valid = true
    left join
        prep_user as blocked_user
        on prep_namespace.creator_id = blocked_user.dim_user_id
    {%- endif %}
    {%- if event_cte.user_column_name != "NULL" %}
    left join
        prep_user
        on {{ event_cte.source_cte_name }}.{{ event_cte.user_column_name }}
        = prep_user.dim_user_id
    {%- endif %}
    {%- if event_cte.project_column_name != "NULL" or event_cte.ultimate_parent_namespace_column_name != "NULL" %}
    left join
        prep_plan on {{ event_cte.source_cte_name }}.dim_plan_id = prep_plan.dim_plan_id
    {%- endif %}
    where
        date_part(
            'year', {{ event_cte.source_cte_name }}.created_at
        ) = {{ year_value }} and date_part(
            'month', {{ event_cte.source_cte_name }}.created_at
        ) = {{ month_value }}
    {% if not loop.last %} union all {% endif %}
    {%- endfor %}

)

select *
from data

{{ config({"materialized": "incremental"}) }}

/*
  Each dict must have ALL of the following:
    * event_name
    * primary_key
    * stage_name": "create",
    * "is_representative_of_stage
    * primary_key
  Must have ONE of the following:
    * source_cte_name OR source_table_name
    * key_to_parent_project OR key_to_group_project (NOT both, see how clusters_applications_helm is included twice for group and project.
*/
{%- set event_ctes = [
    {
        "event_name": "action_monthly_active_users_project_repo",
        "source_table_name": "gitlab_dotcom_events",
        "user_column_name": "author_id",
        "key_to_parent_project": "project_id",
        "primary_key": "event_id",
        "stage_name": "create",
        "is_representative_of_stage": "True",
    },
    {
        "event_name": "action_monthly_active_users_design_management",
        "source_table_name": "gitlab_dotcom_events",
        "user_column_name": "author_id",
        "key_to_parent_project": "project_id",
        "primary_key": "event_id",
        "stage_name": "create",
        "is_representative_of_stage": "False",
    },
    {
        "event_name": "action_monthly_active_users_wiki_repo",
        "source_table_name": "gitlab_dotcom_events",
        "user_column_name": "author_id",
        "key_to_parent_project": "project_id",
        "primary_key": "event_id",
        "stage_name": "create",
        "is_representative_of_stage": "False",
    },
    {
        "event_name": "api_fuzzing",
        "source_cte_name": "api_fuzzing_jobs",
        "user_column_name": "ci_build_user_id",
        "key_to_parent_project": "ci_build_project_id",
        "primary_key": "ci_build_id",
        "stage_name": "secure",
        "is_representative_of_stage": "False",
    },
    {
        "event_name": "boards",
        "source_table_name": "gitlab_dotcom_boards",
        "user_column_name": "NULL",
        "key_to_parent_project": "project_id",
        "primary_key": "board_id",
        "stage_name": "plan",
        "is_representative_of_stage": "False",
    },
    {
        "event_name": "ci_builds",
        "source_table_name": "gitlab_dotcom_ci_builds",
        "user_column_name": "ci_build_user_id",
        "key_to_parent_project": "ci_build_project_id",
        "primary_key": "ci_build_id",
        "stage_name": "verify",
        "is_representative_of_stage": "False",
    },
    {
        "event_name": "ci_pipeline_schedules",
        "source_table_name": "gitlab_dotcom_ci_pipeline_schedules",
        "user_column_name": "owner_id",
        "key_to_parent_project": "project_id",
        "primary_key": "ci_pipeline_schedule_id",
        "stage_name": "verify",
        "is_representative_of_stage": "False",
    },
    {
        "event_name": "ci_pipelines",
        "source_table_name": "gitlab_dotcom_ci_pipelines",
        "user_column_name": "user_id",
        "key_to_parent_project": "project_id",
        "primary_key": "ci_pipeline_id",
        "stage_name": "verify",
        "is_representative_of_stage": "True",
    },
    {
        "event_name": "ci_stages",
        "source_table_name": "gitlab_dotcom_ci_stages",
        "user_column_name": "NULL",
        "key_to_parent_project": "project_id",
        "primary_key": "ci_stage_id",
        "stage_name": "configure",
        "is_representative_of_stage": "False",
    },
    {
        "event_name": "ci_triggers",
        "source_table_name": "gitlab_dotcom_ci_triggers",
        "user_column_name": "owner_id",
        "key_to_parent_project": "project_id",
        "primary_key": "ci_trigger_id",
        "stage_name": "verify",
        "is_representative_of_stage": "False",
    },
    {
        "event_name": "clusters_applications_helm",
        "source_table_name": "gitlab_dotcom_clusters_applications_helm_xf",
        "user_column_name": "user_id",
        "key_to_parent_project": "cluster_project_id",
        "primary_key": "clusters_applications_helm_id",
        "stage_name": "configure",
        "is_representative_of_stage": "True",
    },
    {
        "event_name": "container_scanning",
        "source_cte_name": "container_scanning_jobs",
        "user_column_name": "ci_build_user_id",
        "key_to_parent_project": "ci_build_project_id",
        "primary_key": "ci_build_id",
        "stage_name": "secure",
        "is_representative_of_stage": "False",
    },
    {
        "event_name": "dast",
        "source_cte_name": "dast_jobs",
        "user_column_name": "ci_build_user_id",
        "key_to_parent_project": "ci_build_project_id",
        "primary_key": "ci_build_id",
        "stage_name": "secure",
        "is_representative_of_stage": "False",
    },
    {
        "event_name": "dependency_scanning",
        "source_cte_name": "dependency_scanning_jobs",
        "user_column_name": "ci_build_user_id",
        "key_to_parent_project": "ci_build_project_id",
        "primary_key": "ci_build_id",
        "stage_name": "secure",
        "is_representative_of_stage": "False",
    },
    {
        "event_name": "deployments",
        "source_table_name": "gitlab_dotcom_deployments",
        "user_column_name": "user_id",
        "key_to_parent_project": "project_id",
        "primary_key": "deployment_id",
        "stage_name": "release",
        "is_representative_of_stage": "True",
    },
    {
        "event_name": "environments",
        "source_table_name": "gitlab_dotcom_environments",
        "user_column_name": "NULL",
        "key_to_parent_project": "project_id",
        "primary_key": "environment_id",
        "stage_name": "release",
        "is_representative_of_stage": "False",
    },
    {
        "event_name": "epics",
        "source_table_name": "gitlab_dotcom_epics",
        "user_column_name": "author_id",
        "key_to_parent_group": "group_id",
        "primary_key": "epic_id",
        "stage_name": "plan",
        "is_representative_of_stage": "False",
    },
    {
        "event_name": "epic_notes",
        "source_cte_name": "epic_notes",
        "user_column_name": "note_author_id",
        "key_to_parent_group": "ultimate_parent_id",
        "primary_key": "author_id",
        "stage_name": "plan",
        "is_representative_of_stage": "False",
    },
    {
        "event_name": "events",
        "source_table_name": "gitlab_dotcom_events",
        "user_column_name": "author_id",
        "key_to_parent_group": "project_id",
        "primary_key": "event_id",
        "stage_name": "manage",
        "is_representative_of_stage": "False",
    },
    {
        "event_name": "groups",
        "source_cte_name": "group_members",
        "user_column_name": "user_id",
        "key_to_parent_project": "source_id",
        "primary_key": "member_id",
        "stage_name": "manage",
        "is_representative_of_stage": "False",
    },
    {
        "event_name": "incident_labeled_issues",
        "source_cte_name": "incident_labeled_issues",
        "user_column_name": "author_id",
        "key_to_parent_project": "project_id",
        "primary_key": "issue_id",
        "stage_name": "monitor",
        "is_representative_of_stage": "False",
    },
    {
        "event_name": "issues",
        "source_table_name": "gitlab_dotcom_issues",
        "user_column_name": "author_id",
        "key_to_parent_project": "project_id",
        "primary_key": "issue_id",
        "stage_name": "plan",
        "is_representative_of_stage": "True",
    },
    {
        "event_name": "issue_notes",
        "source_cte_name": "issue_notes",
        "user_column_name": "note_author_id",
        "key_to_parent_project": "project_id",
        "primary_key": "author_id",
        "stage_name": "plan",
        "is_representative_of_stage": "False",
    },
    {
        "event_name": "issue_resource_label_events",
        "source_cte_name": "issue_resource_label_events",
        "user_column_name": "user_id",
        "key_to_parent_project": "namespace_id",
        "primary_key": "issue_resource_label_event_id",
        "stage_name": "plan",
        "is_representative_of_stage": "False",
    },
    {
        "event_name": "issue_resource_weight_events",
        "source_table_name": "gitlab_dotcom_resource_weight_events_xf",
        "user_column_name": "user_id",
        "key_to_parent_project": "project_id",
        "primary_key": "resource_weight_event_id",
        "stage_name": "plan",
        "is_representative_of_stage": "False",
    },
    {
        "event_name": "issue_resource_milestone_events",
        "source_cte_name": "issue_resource_milestone_events",
        "user_column_name": "user_id",
        "key_to_parent_project": "project_id",
        "primary_key": "resource_milestone_event_id",
        "stage_name": "plan",
        "is_representative_of_stage": "False",
    },
    {
        "event_name": "labels",
        "source_table_name": "gitlab_dotcom_labels",
        "user_column_name": "NULL",
        "key_to_parent_project": "project_id",
        "primary_key": "label_id",
        "stage_name": "plan",
        "is_representative_of_stage": "False",
    },
    {
        "event_name": "lfs_objects",
        "source_table_name": "gitlab_dotcom_lfs_objects_projects",
        "user_column_name": "NULL",
        "key_to_parent_project": "project_id",
        "primary_key": "lfs_object_id",
        "stage_name": "create",
        "is_representative_of_stage": "False",
    },
    {
        "event_name": "license_scanning",
        "source_cte_name": "license_scanning_jobs",
        "user_column_name": "ci_build_user_id",
        "key_to_parent_project": "ci_build_project_id",
        "primary_key": "ci_build_id",
        "stage_name": "secure",
        "is_representative_of_stage": "False",
    },
    {
        "event_name": "merge_requests",
        "source_table_name": "gitlab_dotcom_merge_requests",
        "user_column_name": "author_id",
        "key_to_parent_project": "project_id",
        "primary_key": "merge_request_id",
        "stage_name": "create",
        "is_representative_of_stage": "False",
    },
    {
        "event_name": "merge_request_notes",
        "source_cte_name": "merge_request_notes",
        "user_column_name": "note_author_id",
        "key_to_parent_project": "project_id",
        "primary_key": "author_id",
        "stage_name": "create",
        "is_representative_of_stage": "False",
    },
    {
        "event_name": "milestones",
        "source_table_name": "gitlab_dotcom_milestones",
        "user_column_name": "NULL",
        "key_to_parent_project": "project_id",
        "primary_key": "milestone_id",
        "stage_name": "plan",
        "is_representative_of_stage": "False",
    },
    {
        "event_name": "notes",
        "source_table_name": "gitlab_dotcom_notes",
        "user_column_name": "note_author_id",
        "key_to_parent_project": "project_id",
        "primary_key": "note_id",
        "stage_name": "plan",
        "is_representative_of_stage": "False",
    },
    {
        "event_name": "packages",
        "source_table_name": "gitlab_dotcom_packages_packages",
        "user_column_name": "creator_id",
        "key_to_parent_project": "project_id",
        "primary_key": "packages_package_id",
        "stage_name": "package",
        "is_representative_of_stage": "True",
    },
    {
        "event_name": "project_auto_devops",
        "source_table_name": "gitlab_dotcom_project_auto_devops",
        "user_column_name": "NULL",
        "key_to_parent_project": "project_id",
        "primary_key": "project_auto_devops_id",
        "stage_name": "configure",
        "is_representative_of_stage": "False",
    },
    {
        "event_name": "projects_container_registry_enabled",
        "source_cte_name": "projects_container_registry_enabled",
        "user_column_name": "creator_id",
        "key_to_parent_project": "project_id",
        "primary_key": "project_id",
        "stage_name": "package",
        "is_representative_of_stage": "False",
    },
    {
        "event_name": "projects_prometheus_active",
        "source_cte_name": "projects_prometheus_active",
        "user_column_name": "creator_id",
        "key_to_parent_project": "project_id",
        "primary_key": "project_id",
        "stage_name": "monitor",
        "is_representative_of_stage": "True",
    },
    {
        "event_name": "push_events",
        "source_cte_name": "push_events",
        "user_column_name": "author_id",
        "key_to_parent_project": "project_id",
        "primary_key": "project_id",
        "stage_name": "create",
        "is_representative_of_stage": "False",
    },
    {
        "event_name": "releases",
        "source_table_name": "gitlab_dotcom_releases",
        "user_column_name": "author_id",
        "key_to_parent_project": "project_id",
        "primary_key": "release_id",
        "stage_name": "release",
        "is_representative_of_stage": "False",
    },
    {
        "event_name": "requirements",
        "source_table_name": "gitlab_dotcom_requirements",
        "user_column_name": "author_id",
        "key_to_parent_project": "project_id",
        "primary_key": "requirement_id",
        "stage_name": "plan",
        "is_representative_of_stage": "False",
    },
    {
        "event_name": "sast",
        "source_cte_name": "sast_jobs",
        "user_column_name": "ci_build_user_id",
        "key_to_parent_project": "ci_build_project_id",
        "primary_key": "ci_build_id",
        "stage_name": "secure",
        "is_representative_of_stage": "False",
    },
    {
        "event_name": "secret_detection",
        "source_cte_name": "secret_detection_jobs",
        "user_column_name": "ci_build_user_id",
        "key_to_parent_project": "ci_build_project_id",
        "primary_key": "ci_build_id",
        "stage_name": "secure",
        "is_representative_of_stage": "True",
    },
    {
        "event_name": "secure_stage_ci_jobs",
        "source_table_name": "gitlab_dotcom_secure_stage_ci_jobs",
        "user_column_name": "ci_build_user_id",
        "key_to_parent_project": "ci_build_project_id",
        "primary_key": "ci_build_id",
        "stage_name": "secure",
        "is_representative_of_stage": "False",
    },
    {
        "event_name": "services",
        "source_cte_name": "services",
        "user_column_name": "NULL",
        "key_to_parent_project": "project_id",
        "primary_key": "service_id",
        "stage_name": "create",
        "is_representative_of_stage": "False",
    },
    {
        "event_name": "snippets",
        "source_table_name": "gitlab_dotcom_snippets",
        "user_column_name": "author_id",
        "key_to_parent_project": "project_id",
        "primary_key": "snippet_id",
        "stage_name": "create",
        "is_representative_of_stage": "False",
    },
    {
        "event_name": "terraform_reports",
        "source_cte_name": "terraform_reports",
        "user_column_name": "NULL",
        "key_to_parent_project": "project_id",
        "primary_key": "ci_job_artifact_id",
        "stage_name": "configure",
        "is_representative_of_stage": "False",
    },
    {
        "event_name": "todos",
        "source_table_name": "gitlab_dotcom_todos",
        "user_column_name": "author_id",
        "key_to_parent_project": "project_id",
        "primary_key": "todo_id",
        "stage_name": "plan",
        "is_representative_of_stage": "False",
    },
    {
        "event_name": "users",
        "source_table_name": "gitlab_dotcom_users",
        "user_column_name": "user_id",
        "primary_key": "user_id",
        "stage_name": "manage",
        "is_representative_of_stage": "True",
    },
] -%}


with
    gitlab_subscriptions as (

        select *
        from {{ ref("gitlab_dotcom_gitlab_subscriptions_snapshots_namespace_id_base") }}

    )

    ,
    namespaces as (select * from {{ ref("gitlab_dotcom_namespaces_xf") }})

    ,
    plans as (select * from {{ ref("gitlab_dotcom_plans") }})

    ,
    projects as (select * from {{ ref("gitlab_dotcom_projects_xf") }})

    ,
    users as (select * from {{ ref("gitlab_dotcom_users") }})


    /* Source CTEs Start Here */
    ,
    action_monthly_active_users_project_repo as (

        select *
        from {{ ref("gitlab_dotcom_events") }}
        where target_type is null and event_action_type_id = 5


    ),
    action_monthly_active_users_design_management as (

        select *
        from {{ ref("gitlab_dotcom_events") }}
        where
            target_type = 'DesignManagement::Design' and event_action_type_id in (1, 2)

    ),
    action_monthly_active_users_wiki_repo as (

        select *
        from {{ ref("gitlab_dotcom_events") }}
        where target_type = 'WikiPage::Meta' and event_action_type_id in (1, 2)

    ),
    api_fuzzing_jobs as (

        select *
        from {{ ref("gitlab_dotcom_secure_stage_ci_jobs") }}
        where secure_ci_job_type = 'api_fuzzing'

    ),
    container_scanning_jobs as (

        select *
        from {{ ref("gitlab_dotcom_secure_stage_ci_jobs") }}
        where secure_ci_job_type = 'container_scanning'

    ),
    dast_jobs as (

        select *
        from {{ ref("gitlab_dotcom_secure_stage_ci_jobs") }}
        where secure_ci_job_type = 'dast'

    ),
    dependency_scanning_jobs as (

        select *
        from {{ ref("gitlab_dotcom_secure_stage_ci_jobs") }}
        where secure_ci_job_type = 'dependency_scanning'

    ),
    epic_notes as (select * from {{ ref("gitlab_dotcom_epic_notes_xf") }}),
    incident_labeled_issues as (

        select *, issue_created_at as created_at
        from {{ ref("gitlab_dotcom_issues_xf") }}
        where array_contains('incident'::variant, labels)

    ),
    issue_notes as (

        select * from {{ ref("gitlab_dotcom_notes") }} where noteable_type = 'Issue'

    ),
    issue_resource_label_events as (

        select *
        from {{ ref("gitlab_dotcom_resource_label_events_xf") }}
        where issue_id is not null

    ),
    issue_resource_milestone_events as (

        select *
        from {{ ref("gitlab_dotcom_resource_milestone_events_xf") }}
        where issue_id is not null

    ),
    license_scanning_jobs as (

        select *
        from {{ ref("gitlab_dotcom_secure_stage_ci_jobs") }}
        where secure_ci_job_type in ('license_scanning', 'license_management')

    ),
    merge_request_notes as (

        select *
        from {{ ref("gitlab_dotcom_notes") }}
        where noteable_type = 'MergeRequest'

    ),
    projects_prometheus_active as (

        select *
        from {{ ref("gitlab_dotcom_projects_xf") }}
        where array_contains('PrometheusService'::variant, active_service_types)

    ),
    projects_prometheus_active as (

        select *
        from {{ ref("gitlab_dotcom_projects_xf") }}
        where array_contains('PrometheusService'::variant, active_service_types)

    ),
    projects_container_registry_enabled as (

        select *
        from {{ ref("gitlab_dotcom_projects_xf") }}
        where container_registry_enabled = true

    ),
    push_events as (

        select *
        from {{ ref("gitlab_dotcom_events") }}
        where event_action_type = 'pushed'

    ),
    group_members as (

        select *, invite_created_at as created_at
        from {{ ref("gitlab_dotcom_members") }}
        where member_source_type = 'Namespace'

    ),
    sast_jobs as (

        select *
        from {{ ref("gitlab_dotcom_secure_stage_ci_jobs") }}
        where secure_ci_job_type = 'sast'

    ),
    secret_detection_jobs as (

        select *
        from {{ ref("gitlab_dotcom_secure_stage_ci_jobs") }}
        where secure_ci_job_type = 'secret_detection'

    ),
    services as (

        select *
        from {{ ref("gitlab_dotcom_services") }}
        where service_type != 'GitlabIssueTrackerService'

    ),
    terraform_reports as (

        select * from {{ ref("gitlab_dotcom_ci_job_artifacts") }} where file_type = 18

    )
    /* End of Source CTEs */
    {% for event_cte in event_ctes %}

    ,
    {{ event_cte.event_name }} as (

        select *
        /* Check for source_table_name, else use source_cte_name. */
        {% if event_cte.source_table_name is defined %}
        from {{ ref(event_cte.source_table_name) }}
        {% else %} from {{ event_cte.source_cte_name }}
        {% endif %}
        where
            created_at is not null
            {% if is_incremental() %}
            and created_at >= (
                select max(event_created_at)
                from {{ this }}
                where event_name = '{{ event_cte.event_name }}'
            )
            {% endif %}

    )

    {% endfor -%}

    ,
    data as (

        {% for event_cte in event_ctes %}

        select
            ultimate_namespace.namespace_id,
            ultimate_namespace.namespace_created_at,
            {% if "NULL" in event_cte.user_column_name %} null
            {% else %} {{ event_cte.event_name }}.{{ event_cte.user_column_name }}
            {% endif %} as user_id,
            {% if event_cte.key_to_parent_project is defined %}
            'project' as parent_type,
            projects.project_id as parent_id,
            projects.project_created_at as parent_created_at,
            {% elif event_cte.key_to_parent_group is defined %}
            'group' as parent_type,
            namespaces.namespace_id as parent_id,
            namespaces.namespace_created_at as parent_created_at,
            {% else %}
            null as parent_type, null as parent_id, null as parent_created_at,
            {% endif %}
            ultimate_namespace.namespace_is_internal as namespace_is_internal,
            {{ event_cte.event_name }}.created_at as event_created_at,
            {{ event_cte.is_representative_of_stage }}::boolean
            as is_representative_of_stage,
            '{{ event_cte.event_name }}' as event_name,
            '{{ event_cte.stage_name }}' as stage_name,
            case
                when gitlab_subscriptions.is_trial
                then 'trial'
                else coalesce(gitlab_subscriptions.plan_id, 34)::varchar
            end as plan_id_at_event_date,
            case
                when gitlab_subscriptions.is_trial
                then 'trial'
                else coalesce(plans.plan_name, 'free')
            end as plan_name_at_event_date
        from {{ event_cte.event_name }}
        /* Join with parent project. */
        {% if event_cte.key_to_parent_project is defined %}
        left join
            projects
            on {{ event_cte.event_name }}.{{ event_cte.key_to_parent_project }}
            = projects.project_id
        /* Join with parent group. */
        {% elif event_cte.key_to_parent_group is defined %}
        left join
            namespaces
            on {{ event_cte.event_name }}.{{ event_cte.key_to_parent_group }}
            = namespaces.namespace_id
        {% endif %}

        -- Join on either the project's or the group's ultimate namespace.
        left join
            namespaces as ultimate_namespace
            {% if event_cte.key_to_parent_project is defined %}
            on ultimate_namespace.namespace_id = projects.ultimate_parent_id
            {% elif event_cte.key_to_parent_group is defined %}
            on ultimate_namespace.namespace_id = namespaces.namespace_ultimate_parent_id
            {% else %} on false  -- Don't join any rows.
            {% endif %}

        left join
            gitlab_subscriptions
            on ultimate_namespace.namespace_id = gitlab_subscriptions.namespace_id
            and {{ event_cte.event_name }}.created_at
            between gitlab_subscriptions.valid_from
            and {{ coalesce_to_infinity("gitlab_subscriptions.valid_to") }}
        left join plans on gitlab_subscriptions.plan_id = plans.plan_id
        {% if "NULL" not in event_cte.user_column_name %}
        where
            {{
                filter_out_active_users(
                    event_cte.event_name, event_cte.user_column_name
                )
            }}
        {% endif %}


        {% if not loop.last %} union {% endif %}
        {% endfor -%}

    ),
    final as (
        select
            data.*,
            users.created_at as user_created_at,
            floor(
                datediff('hour', namespace_created_at, event_created_at) / 24
            ) as days_since_namespace_creation,
            floor(
                datediff('hour', namespace_created_at, event_created_at) / (24 * 7)
            ) as weeks_since_namespace_creation,
            floor(
                datediff('hour', parent_created_at, event_created_at) / 24
            ) as days_since_parent_creation,
            floor(
                datediff('hour', parent_created_at, event_created_at) / (24 * 7)
            ) as weeks_since_parent_creation,
            floor(
                datediff('hour', user_created_at, event_created_at) / 24
            ) as days_since_user_creation,
            floor(
                datediff('hour', user_created_at, event_created_at) / (24 * 7)
            ) as weeks_since_user_creation
        from data
        left join users on data.user_id = users.user_id

    )

select *
from final

{{ config({"materialized": "table"}) }}


with
    postgres_counts as (

        select table_name, created_date, updated_date, number_of_records
        from {{ source("gitlab_dotcom", "gitlab_pgp_export") }}
        where
            table_name not in (
                'gitlab_db_operations_feature_flags',
                'gitlab_db_requirements_management_test_reports',
                'gitlab_db_resource_milestone_events',
                'gitlab_db_resource_weight_events',
                'gitlab_db_authentication_events',
                'gitlab_db_uploads',
                'gitlab_db_resource_label_events',
                'gitlab_db_lfs_file_locks',
                'gitlab_db_project_daily_statistics',
                'gitlab_db_audit_events',
                'gitlab_db_ci_platform_metrics',
                'gitlab_db_namespace_root_storage_statistics',
                'gitlab_ops_db_ci_stages'
            )
        group by 1, 2, 3, 4
        qualify
            row_number() over (
                partition by table_name, created_date, updated_date
                order by updated_date desc
            )
            = 1
        order by table_name, updated_date desc
    ),
    date_check as (

        select table_name, dateadd(day, -8, max(updated_date)) as updated_date
        from {{ source("gitlab_dotcom", "gitlab_pgp_export") }}
        group by 1
    ),
    sub_group as (

        {% set tables = [
    "label_priorities",
    "labels",
    "ldap_group_links",
    "namespaces",
    "packages_packages",
    "ci_runner_projects",
    "push_rules",
    "requirements",
    "todos",
    "project_auto_devops",
    "application_settings",
    "ci_triggers",
    "clusters_applications_elastic_stacks",
    "users",
    "zoom_meetings",
    "alert_management_http_integrations",
    "approval_project_rules",
    "clusters",
    "issue_metrics",
    "jira_tracker_data",
    "lists",
    "sprints",
    "users_ops_dashboard_projects",
    "bulk_imports",
    "cluster_agent_tokens",
    "experiment_users",
    "protected_branches",
    "timelogs",
    "project_features",
    "milestones",
    "alert_management_alerts",
    "ci_group_variables",
    "cluster_agents",
    "emails",
    "user_custom_attributes",
    "grafana_integrations",
    "security_scans",
    "lfs_objects_projects",
    "merge_request_metrics",
    "merge_requests_closing_issues",
    "path_locks",
    "approval_merge_request_rules",
    "csv_issue_imports",
    "cluster_projects",
    "vulnerabilities",
    "releases",
    "subscriptions",
    "terraform_states",
    "project_tracing_settings",
    "notification_settings",
    "environments",
    "epics",
    "in_product_marketing_emails",
    "jira_imports",
    "services",
    "onboarding_progresses",
    "project_custom_attributes",
    "analytics_cycle_analytics_group_stages",
    "approvals",
    "ci_pipeline_schedule_variables",
    "ci_runners",
    "ci_trigger_requests",
    "boards",
    "projects",
    "identities",
    "lfs_objects",
    "prometheus_alerts",
    "snippets",
    "system_note_metadata",
    "merge_request_blocks",
    "merge_request_diffs",
    "experiment_subjects",
    "deployments",
    "merge_requests",
    "remote_mirrors",
    "integrations",
    "events",
    "ci_stages",
    "ci_pipelines",
    "ci_job_artifacts",
    "ci_pipeline_schedules",
    "approver_groups",
    "boards_epic_boards",
    "web_hooks",
    "routes",
    "notes",
    "issues",
    "status_page_published_incidents",
    "epic_metrics",
    "dast_profiles",
    "notes",
    "ci_builds",
] %}

        {% for table in tables %}
        select
            snowflake.id,
            'gitlab_db_{{table}}' as table_name,
            date(snowflake.created_at) as created_date,
            date(snowflake.updated_at) as updated_date
        from {{ source("gitlab_dotcom", table) }} as snowflake
        inner join
            date_check
            on date(snowflake.updated_at) >= date_check.updated_date
            and date_check.table_name = 'gitlab_db_{{table}}'
        qualify row_number() over (partition by id order by updated_date desc) = 1


        {% if not loop.last %}
        union all
        {% endif %}

        {% endfor %}

        union all

        {% set tables = [
    "labels",
    "merge_request_metrics",
    "projects",
    "merge_requests",
    "users",
    "ci_pipelines",
] %}

        {% for table in tables %}
        select
            snowflake.id,
            'gitlab_ops_db_{{table}}' as table_name,
            date(snowflake.created_at) as created_date,
            date(snowflake.updated_at) as updated_date
        from {{ source("gitlab_ops", table) }} as snowflake
        inner join
            date_check
            on date(snowflake.updated_at) >= date_check.updated_date
            and date_check.table_name = 'gitlab_db_{{table}}'
        qualify row_number() over (partition by id order by updated_date desc) = 1


        {% if not loop.last %}
        union all
        {% endif %}

        {% endfor %}

    ),  -- union all of tables with different column name for id
    final_group as (

        select *
        from sub_group
        union all
        select
            snowflake.issue_id,
            'gitlab_db_issues_prometheus_alert_events' as table_name,
            date(snowflake.created_at) as created_date,
            date(snowflake.updated_at) as updated_date
        from
            {{ source("gitlab_dotcom", "issues_prometheus_alert_events") }} as snowflake
        inner join
            date_check
            on date(snowflake.updated_at) >= date_check.updated_date
            and date_check.table_name = 'gitlab_db_issues_prometheus_alert_events'
        qualify row_number() over (partition by issue_id order by updated_date desc) = 1
        union all
        select
            snowflake.group_id,
            'gitlab_db_group_import_states' as table_name,
            date(snowflake.created_at) as created_date,
            date(snowflake.updated_at) as updated_date
        from {{ source("gitlab_dotcom", "group_import_states") }} as snowflake
        inner join
            date_check
            on date(snowflake.updated_at) >= date_check.updated_date
            and date_check.table_name = 'gitlab_db_group_import_states'
        qualify row_number() over (partition by group_id order by updated_date desc) = 1
        union all
        select
            snowflake.issue_id,
            'gitlab_db_issues_self_managed_prometheus_alert_events' as table_name,
            date(snowflake.created_at) as created_date,
            date(snowflake.updated_at) as updated_date
        from
            {{ source("gitlab_dotcom", "issues_self_managed_prometheus_alert_events") }}
            as snowflake
        inner join
            date_check
            on date(snowflake.updated_at) >= date_check.updated_date
            and date_check.table_name
            = 'gitlab_db_issues_self_managed_prometheus_alert_events'
        qualify row_number() over (partition by issue_id order by updated_date desc) = 1
        union all
        select
            snowflake.project_id,
            'gitlab_db_status_page_settings' as table_name,
            date(snowflake.created_at) as created_date,
            date(snowflake.updated_at) as updated_date
        from {{ source("gitlab_dotcom", "status_page_settings") }} as snowflake
        inner join
            date_check
            on date(snowflake.updated_at) >= date_check.updated_date
            and date_check.table_name = 'gitlab_db_status_page_settings'
        qualify
            row_number() over (partition by project_id order by updated_date desc) = 1
        union all
        select
            snowflake.user_id,
            'gitlab_db_user_preferences' as table_name,
            date(snowflake.created_at) as created_date,
            date(snowflake.updated_at) as updated_date
        from {{ source("gitlab_dotcom", "user_preferences") }} as snowflake
        inner join
            date_check
            on date(snowflake.updated_at) >= date_check.updated_date
            and date_check.table_name = 'gitlab_db_user_preferences'
        qualify row_number() over (partition by user_id order by updated_date desc) = 1
        union all
        select
            snowflake.project_id,
            'gitlab_db_container_expiration_policies' as table_name,
            date(snowflake.created_at) as created_date,
            date(snowflake.updated_at) as updated_date
        from {{ source("gitlab_dotcom", "container_expiration_policies") }} as snowflake
        inner join
            date_check
            on date(snowflake.updated_at) >= date_check.updated_date
            and date_check.table_name = 'gitlab_db_container_expiration_policies'
        qualify
            row_number() over (partition by project_id order by updated_date desc) = 1
        union all
        select
            snowflake.namespace_id,
            'gitlab_db_namespace_settings' as table_name,
            date(snowflake.created_at) as created_date,
            date(snowflake.updated_at) as updated_date
        from {{ source("gitlab_dotcom", "namespace_settings") }} as snowflake
        inner join
            date_check
            on date(snowflake.updated_at) >= date_check.updated_date
            and date_check.table_name = 'gitlab_db_namespace_settings'
        qualify
            row_number() over (partition by namespace_id order by updated_date desc) = 1

    ),
    snowflake_counts as (

        select table_name, created_date, updated_date, count(*) as number_of_records
        from final_group
        group by 1, 2, 3

    ),
    comparision as (

        select
            snowflake_counts.table_name as table_name,
            snowflake_counts.created_date as created_date,
            snowflake_counts.updated_date as updated_date,
            postgres_counts.number_of_records as postgres_counts,
            snowflake_counts.number_of_records as snowflake_counts
        from snowflake_counts
        inner join
            postgres_counts
            on snowflake_counts.table_name = postgres_counts.table_name
            and snowflake_counts.created_date = postgres_counts.created_date
            and snowflake_counts.updated_date
            = substring(postgres_counts.updated_date, 1, 10)
    )

select *, postgres_counts - snowflake_counts as deviation
from comparision
order by table_name, updated_date desc

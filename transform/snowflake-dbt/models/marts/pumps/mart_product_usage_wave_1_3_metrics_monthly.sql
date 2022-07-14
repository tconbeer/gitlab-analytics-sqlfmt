{{ config(tags=["mnpi_exception"]) }}

{{ config({"schema": "common_mart_product"}) }}

{{
    simple_cte(
        [
            ("monthly_metrics", "fct_product_usage_wave_1_3_metrics_monthly"),
            ("billing_accounts", "dim_billing_account"),
            ("crm_accounts", "dim_crm_account"),
            ("location_country", "dim_location_country"),
            ("subscriptions", "dim_subscription_snapshot_bottom_up"),
        ]
    )
}},
original_subscription_dates as (

    select distinct dim_subscription_id, subscription_start_date, subscription_end_date
    from subscriptions
    where subscription_version = 1

),
joined as (

    select
        monthly_metrics.dim_subscription_id,
        monthly_metrics.dim_subscription_id_original,
        subscriptions.subscription_status,
        subscriptions.subscription_start_date,
        subscriptions.subscription_end_date,
        subscriptions_original.subscription_status as subscription_status_original,
        original_subscription_dates.subscription_start_date
        as subscription_start_date_original,
        original_subscription_dates.subscription_end_date
        as subscription_end_date_original,
        {{ get_keyed_nulls("billing_accounts.dim_billing_account_id") }}
        as dim_billing_account_id,
        {{ get_keyed_nulls("crm_accounts.dim_crm_account_id") }} as dim_crm_account_id,
        monthly_metrics.snapshot_month,
        monthly_metrics.snapshot_date_id,
        monthly_metrics.seat_link_report_date,
        monthly_metrics.seat_link_report_date_id,
        monthly_metrics.dim_usage_ping_id,
        monthly_metrics.ping_created_at,
        monthly_metrics.ping_created_date_id,
        monthly_metrics.uuid,
        monthly_metrics.hostname,
        monthly_metrics.instance_type,
        monthly_metrics.dim_license_id,
        monthly_metrics.license_md5,
        monthly_metrics.cleaned_version,
        location_country.country_name,
        location_country.iso_2_country_code,
        location_country.iso_3_country_code,
        -- Wave 1
        monthly_metrics.license_utilization,
        monthly_metrics.active_user_count,
        monthly_metrics.max_historical_user_count,
        monthly_metrics.license_user_count,
        -- Wave 2 & 3
        monthly_metrics.umau_28_days_user,
        monthly_metrics.action_monthly_active_users_project_repo_28_days_user,
        monthly_metrics.merge_requests_28_days_user,
        monthly_metrics.projects_with_repositories_enabled_28_days_user,
        monthly_metrics.commit_comment_all_time_event,
        monthly_metrics.source_code_pushes_all_time_event,
        monthly_metrics.ci_pipelines_28_days_user,
        monthly_metrics.ci_internal_pipelines_28_days_user,
        monthly_metrics.ci_builds_28_days_user,
        monthly_metrics.ci_builds_all_time_user,
        monthly_metrics.ci_builds_all_time_event,
        monthly_metrics.ci_runners_all_time_event,
        monthly_metrics.auto_devops_enabled_all_time_event,
        monthly_metrics.gitlab_shared_runners_enabled,
        monthly_metrics.container_registry_enabled,
        monthly_metrics.template_repositories_all_time_event,
        monthly_metrics.ci_pipeline_config_repository_28_days_user,
        monthly_metrics.user_unique_users_all_secure_scanners_28_days_user,
        monthly_metrics.user_sast_jobs_28_days_user,
        monthly_metrics.user_dast_jobs_28_days_user,
        monthly_metrics.user_dependency_scanning_jobs_28_days_user,
        monthly_metrics.user_license_management_jobs_28_days_user,
        monthly_metrics.user_secret_detection_jobs_28_days_user,
        monthly_metrics.user_container_scanning_jobs_28_days_user,
        monthly_metrics.object_store_packages_enabled,
        monthly_metrics.projects_with_packages_all_time_event,
        monthly_metrics.projects_with_packages_28_days_user,
        monthly_metrics.deployments_28_days_user,
        monthly_metrics.releases_28_days_user,
        monthly_metrics.epics_28_days_user,
        monthly_metrics.issues_28_days_user,
        -- Wave 3.1
        monthly_metrics.ci_internal_pipelines_all_time_event,
        monthly_metrics.ci_external_pipelines_all_time_event,
        monthly_metrics.merge_requests_all_time_event,
        monthly_metrics.todos_all_time_event,
        monthly_metrics.epics_all_time_event,
        monthly_metrics.issues_all_time_event,
        monthly_metrics.projects_all_time_event,
        monthly_metrics.deployments_28_days_event,
        monthly_metrics.packages_28_days_event,
        monthly_metrics.sast_jobs_all_time_event,
        monthly_metrics.dast_jobs_all_time_event,
        monthly_metrics.dependency_scanning_jobs_all_time_event,
        monthly_metrics.license_management_jobs_all_time_event,
        monthly_metrics.secret_detection_jobs_all_time_event,
        monthly_metrics.container_scanning_jobs_all_time_event,
        monthly_metrics.projects_jenkins_active_all_time_event,
        monthly_metrics.projects_bamboo_active_all_time_event,
        monthly_metrics.projects_jira_active_all_time_event,
        monthly_metrics.projects_drone_ci_active_all_time_event,
        monthly_metrics.projects_github_active_all_time_event,
        monthly_metrics.projects_jira_server_active_all_time_event,
        monthly_metrics.projects_jira_dvcs_cloud_active_all_time_event,
        monthly_metrics.projects_with_repositories_enabled_all_time_event,
        monthly_metrics.protected_branches_all_time_event,
        monthly_metrics.remote_mirrors_all_time_event,
        monthly_metrics.projects_enforcing_code_owner_approval_28_days_user,
        monthly_metrics.project_clusters_enabled_28_days_user,
        monthly_metrics.analytics_28_days_user,
        monthly_metrics.issues_edit_28_days_user,
        monthly_metrics.user_packages_28_days_user,
        monthly_metrics.terraform_state_api_28_days_user,
        monthly_metrics.incident_management_28_days_user,
        -- Wave 3.2
        monthly_metrics.auto_devops_enabled,
        monthly_metrics.gitaly_clusters_instance,
        monthly_metrics.epics_deepest_relationship_level_instance,
        monthly_metrics.clusters_applications_cilium_all_time_event,
        monthly_metrics.network_policy_forwards_all_time_event,
        monthly_metrics.network_policy_drops_all_time_event,
        monthly_metrics.requirements_with_test_report_all_time_event,
        monthly_metrics.requirement_test_reports_ci_all_time_event,
        monthly_metrics.projects_imported_from_github_all_time_event,
        monthly_metrics.projects_jira_cloud_active_all_time_event,
        monthly_metrics.projects_jira_dvcs_server_active_all_time_event,
        monthly_metrics.service_desk_issues_all_time_event,
        monthly_metrics.ci_pipelines_all_time_user,
        monthly_metrics.service_desk_issues_28_days_user,
        monthly_metrics.projects_jira_active_28_days_user,
        monthly_metrics.projects_jira_dvcs_cloud_active_28_days_user,
        monthly_metrics.projects_jira_dvcs_server_active_28_days_user,
        monthly_metrics.merge_requests_with_required_code_owners_28_days_user,
        monthly_metrics.analytics_value_stream_28_days_event,
        monthly_metrics.code_review_user_approve_mr_28_days_user,
        monthly_metrics.epics_usage_28_days_user,
        monthly_metrics.ci_templates_usage_28_days_event,
        monthly_metrics.project_management_issue_milestone_changed_28_days_user,
        monthly_metrics.project_management_issue_iteration_changed_28_days_user,
        -- Data Quality Flags
        monthly_metrics.instance_user_count_not_aligned,
        monthly_metrics.historical_max_users_not_aligned,
        monthly_metrics.is_seat_link_subscription_in_zuora,
        monthly_metrics.is_seat_link_rate_plan_in_zuora,
        monthly_metrics.is_seat_link_active_user_count_available,
        monthly_metrics.is_usage_ping_license_mapped_to_subscription,
        monthly_metrics.is_usage_ping_license_subscription_id_valid,
        monthly_metrics.is_data_in_subscription_month,
        monthly_metrics.is_latest_data
    from monthly_metrics
    left join
        billing_accounts
        on monthly_metrics.dim_billing_account_id
        = billing_accounts.dim_billing_account_id
    left join
        crm_accounts
        on billing_accounts.dim_crm_account_id = crm_accounts.dim_crm_account_id
    left join
        location_country
        on monthly_metrics.dim_location_country_id
        = location_country.dim_location_country_id
    left join
        subscriptions
        on monthly_metrics.dim_subscription_id = subscriptions.dim_subscription_id
        and ifnull(
            monthly_metrics.ping_created_at::date,
            dateadd('day', -1, monthly_metrics.snapshot_month)
        ) = to_date(to_char(subscriptions.snapshot_id), 'YYYYMMDD')
    left join
        subscriptions as subscriptions_original
        on monthly_metrics.dim_subscription_id_original
        = subscriptions_original.dim_subscription_id_original
        and ifnull(
            monthly_metrics.ping_created_at::date,
            dateadd('day', -1, monthly_metrics.snapshot_month)
        ) = to_date(to_char(subscriptions_original.snapshot_id), 'YYYYMMDD')
    left join
        original_subscription_dates
        on original_subscription_dates.dim_subscription_id
        = monthly_metrics.dim_subscription_id_original

)

{{
    dbt_audit(
        cte_ref="joined",
        created_by="@ischweickartDD",
        updated_by="@mdrussell",
        created_date="2021-02-11",
        updated_date="2021-12-23",
    )
}}

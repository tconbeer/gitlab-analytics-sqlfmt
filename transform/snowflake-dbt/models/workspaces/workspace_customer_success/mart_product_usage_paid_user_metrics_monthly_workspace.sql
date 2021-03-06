{{ config(materialized="table", tags=["mnpi_exception"]) }}

{{
    simple_cte(
        [
            ("monthly_saas_metrics", "fct_saas_product_usage_metrics_monthly"),
            ("monthly_sm_metrics", "fct_product_usage_wave_1_3_metrics_monthly"),
            ("billing_accounts", "dim_billing_account"),
            ("location_country", "dim_location_country"),
            ("subscriptions", "dim_subscription"),
            ("namespaces", "dim_namespace"),
            ("charges", "mart_charge"),
            ("dates", "dim_date"),
        ]
    )
}},
most_recent_subscription_version as (
    select
        subscription_name,
        subscription_status,
        subscription_start_date,
        subscription_end_date,
        row_number() over (
            partition by subscription_name order by subscription_version desc
        )
    from subscriptions
    where subscription_status in ('Active', 'Cancelled')
    qualify
        row_number() over (
            partition by subscription_name order by subscription_version desc
        )
        = 1

),
zuora_licenses_per_subscription as (

    select
        dates.first_day_of_month as month,
        subscriptions.dim_subscription_id_original,
        sum(charges.quantity) as zuora_licenses
    from charges
    join
        dates
        on charges.effective_start_month <= dates.date_actual
        and (
            charges.effective_end_month > dates.date_actual
            or charges.effective_end_month is null
        )
        and dates.day_of_month = 1
    left join
        subscriptions on charges.dim_subscription_id = subscriptions.dim_subscription_id
    where
        charges.subscription_status in ('Active', 'Cancelled')
        and charges.product_tier_name != 'Storage'
        {{ dbt_utils.group_by(n=2) }}

),
sm_paid_user_metrics as (

    select
        monthly_sm_metrics.snapshot_month,
        monthly_sm_metrics.dim_subscription_id,
        null as dim_namespace_id,
        null as namespace_name,
        monthly_sm_metrics.uuid,
        monthly_sm_metrics.hostname,
        {{ get_keyed_nulls("billing_accounts.dim_billing_account_id") }}
        as dim_billing_account_id,
        {{ get_keyed_nulls("billing_accounts.dim_crm_account_id") }}
        as dim_crm_account_id,
        monthly_sm_metrics.dim_subscription_id_original,
        subscriptions.subscription_name,
        subscriptions.subscription_status,
        most_recent_subscription_version.subscription_status
        as subscription_status_most_recent_version,
        subscriptions.term_start_date,
        subscriptions.term_end_date,
        most_recent_subscription_version.subscription_start_date,
        most_recent_subscription_version.subscription_end_date,
        monthly_sm_metrics.snapshot_date_id,
        monthly_sm_metrics.ping_created_at,
        monthly_sm_metrics.dim_usage_ping_id,
        monthly_sm_metrics.instance_type,
        monthly_sm_metrics.cleaned_version,
        location_country.country_name,
        location_country.iso_2_country_code,
        location_country.iso_3_country_code,
        'Self-Managed' as delivery_type,
        -- Wave 1
        monthly_sm_metrics.license_utilization,
        monthly_sm_metrics.billable_user_count,
        monthly_sm_metrics.active_user_count,
        monthly_sm_metrics.max_historical_user_count,
        monthly_sm_metrics.license_user_count,
        -- Wave 2 & 3
        monthly_sm_metrics.umau_28_days_user,
        monthly_sm_metrics.action_monthly_active_users_project_repo_28_days_user,
        monthly_sm_metrics.merge_requests_28_days_user,
        monthly_sm_metrics.projects_with_repositories_enabled_28_days_user,
        monthly_sm_metrics.commit_comment_all_time_event,
        monthly_sm_metrics.source_code_pushes_all_time_event,
        monthly_sm_metrics.ci_pipelines_28_days_user,
        monthly_sm_metrics.ci_internal_pipelines_28_days_user,
        monthly_sm_metrics.ci_builds_28_days_user,
        monthly_sm_metrics.ci_builds_all_time_user,
        monthly_sm_metrics.ci_builds_all_time_event,
        monthly_sm_metrics.ci_runners_all_time_event,
        monthly_sm_metrics.auto_devops_enabled_all_time_event,
        monthly_sm_metrics.gitlab_shared_runners_enabled,
        monthly_sm_metrics.container_registry_enabled,
        monthly_sm_metrics.template_repositories_all_time_event,
        monthly_sm_metrics.ci_pipeline_config_repository_28_days_user,
        monthly_sm_metrics.user_unique_users_all_secure_scanners_28_days_user,
        monthly_sm_metrics.user_sast_jobs_28_days_user,
        monthly_sm_metrics.user_dast_jobs_28_days_user,
        monthly_sm_metrics.user_dependency_scanning_jobs_28_days_user,
        monthly_sm_metrics.user_license_management_jobs_28_days_user,
        monthly_sm_metrics.user_secret_detection_jobs_28_days_user,
        monthly_sm_metrics.user_container_scanning_jobs_28_days_user,
        monthly_sm_metrics.object_store_packages_enabled,
        monthly_sm_metrics.projects_with_packages_all_time_event,
        monthly_sm_metrics.projects_with_packages_28_days_user,
        monthly_sm_metrics.deployments_28_days_user,
        monthly_sm_metrics.releases_28_days_user,
        monthly_sm_metrics.epics_28_days_user,
        monthly_sm_metrics.issues_28_days_user,
        -- Wave 3.1
        monthly_sm_metrics.ci_internal_pipelines_all_time_event,
        monthly_sm_metrics.ci_external_pipelines_all_time_event,
        monthly_sm_metrics.merge_requests_all_time_event,
        monthly_sm_metrics.todos_all_time_event,
        monthly_sm_metrics.epics_all_time_event,
        monthly_sm_metrics.issues_all_time_event,
        monthly_sm_metrics.projects_all_time_event,
        monthly_sm_metrics.deployments_28_days_event,
        monthly_sm_metrics.packages_28_days_event,
        monthly_sm_metrics.sast_jobs_all_time_event,
        monthly_sm_metrics.dast_jobs_all_time_event,
        monthly_sm_metrics.dependency_scanning_jobs_all_time_event,
        monthly_sm_metrics.license_management_jobs_all_time_event,
        monthly_sm_metrics.secret_detection_jobs_all_time_event,
        monthly_sm_metrics.container_scanning_jobs_all_time_event,
        monthly_sm_metrics.projects_jenkins_active_all_time_event,
        monthly_sm_metrics.projects_bamboo_active_all_time_event,
        monthly_sm_metrics.projects_jira_active_all_time_event,
        monthly_sm_metrics.projects_drone_ci_active_all_time_event,
        monthly_sm_metrics.projects_github_active_all_time_event,
        monthly_sm_metrics.projects_jira_server_active_all_time_event,
        monthly_sm_metrics.projects_jira_dvcs_cloud_active_all_time_event,
        monthly_sm_metrics.projects_with_repositories_enabled_all_time_event,
        monthly_sm_metrics.protected_branches_all_time_event,
        monthly_sm_metrics.remote_mirrors_all_time_event,
        monthly_sm_metrics.projects_enforcing_code_owner_approval_28_days_user,
        monthly_sm_metrics.project_clusters_enabled_28_days_user,
        monthly_sm_metrics.analytics_28_days_user,
        monthly_sm_metrics.issues_edit_28_days_user,
        monthly_sm_metrics.user_packages_28_days_user,
        monthly_sm_metrics.terraform_state_api_28_days_user,
        monthly_sm_metrics.incident_management_28_days_user,
        -- Wave 3.2
        monthly_sm_metrics.auto_devops_enabled,
        monthly_sm_metrics.gitaly_clusters_instance,
        monthly_sm_metrics.epics_deepest_relationship_level_instance,
        monthly_sm_metrics.clusters_applications_cilium_all_time_event,
        monthly_sm_metrics.network_policy_forwards_all_time_event,
        monthly_sm_metrics.network_policy_drops_all_time_event,
        monthly_sm_metrics.requirements_with_test_report_all_time_event,
        monthly_sm_metrics.requirement_test_reports_ci_all_time_event,
        monthly_sm_metrics.projects_imported_from_github_all_time_event,
        monthly_sm_metrics.projects_jira_cloud_active_all_time_event,
        monthly_sm_metrics.projects_jira_dvcs_server_active_all_time_event,
        monthly_sm_metrics.service_desk_issues_all_time_event,
        monthly_sm_metrics.ci_pipelines_all_time_user,
        monthly_sm_metrics.service_desk_issues_28_days_user,
        monthly_sm_metrics.projects_jira_active_28_days_user,
        monthly_sm_metrics.projects_jira_dvcs_cloud_active_28_days_user,
        monthly_sm_metrics.projects_jira_dvcs_server_active_28_days_user,
        monthly_sm_metrics.merge_requests_with_required_code_owners_28_days_user,
        monthly_sm_metrics.analytics_value_stream_28_days_event,
        monthly_sm_metrics.code_review_user_approve_mr_28_days_user,
        monthly_sm_metrics.epics_usage_28_days_user,
        monthly_sm_metrics.ci_templates_usage_28_days_event,
        monthly_sm_metrics.project_management_issue_milestone_changed_28_days_user,
        monthly_sm_metrics.project_management_issue_iteration_changed_28_days_user,
        -- Wave 5.1
        monthly_sm_metrics.protected_branches_28_days_user,
        monthly_sm_metrics.ci_cd_lead_time_usage_28_days_event,
        monthly_sm_metrics.ci_cd_deployment_frequency_usage_28_days_event,
        monthly_sm_metrics.projects_with_repositories_enabled_all_time_user,
        monthly_sm_metrics.api_fuzzing_jobs_usage_28_days_user,
        monthly_sm_metrics.coverage_fuzzing_pipeline_usage_28_days_event,
        monthly_sm_metrics.api_fuzzing_pipeline_usage_28_days_event,
        monthly_sm_metrics.container_scanning_pipeline_usage_28_days_event,
        monthly_sm_metrics.dependency_scanning_pipeline_usage_28_days_event,
        monthly_sm_metrics.sast_pipeline_usage_28_days_event,
        monthly_sm_metrics.secret_detection_pipeline_usage_28_days_event,
        monthly_sm_metrics.dast_pipeline_usage_28_days_event,
        monthly_sm_metrics.coverage_fuzzing_jobs_28_days_user,
        monthly_sm_metrics.environments_all_time_event,
        monthly_sm_metrics.feature_flags_all_time_event,
        monthly_sm_metrics.successful_deployments_28_days_event,
        monthly_sm_metrics.failed_deployments_28_days_event,
        monthly_sm_metrics.projects_compliance_framework_all_time_event,
        monthly_sm_metrics.commit_ci_config_file_28_days_user,
        monthly_sm_metrics.view_audit_all_time_user,
        -- Wave 5.2
        monthly_sm_metrics.dependency_scanning_jobs_all_time_user,
        monthly_sm_metrics.analytics_devops_adoption_all_time_user,
        monthly_sm_metrics.projects_imported_all_time_event,
        monthly_sm_metrics.preferences_security_dashboard_28_days_user,
        monthly_sm_metrics.web_ide_edit_28_days_user,
        monthly_sm_metrics.auto_devops_pipelines_all_time_event,
        monthly_sm_metrics.projects_prometheus_active_all_time_event,
        monthly_sm_metrics.prometheus_enabled,
        monthly_sm_metrics.prometheus_metrics_enabled,
        monthly_sm_metrics.group_saml_enabled,
        monthly_sm_metrics.jira_issue_imports_all_time_event,
        monthly_sm_metrics.author_epic_all_time_user,
        monthly_sm_metrics.author_issue_all_time_user,
        monthly_sm_metrics.failed_deployments_28_days_user,
        monthly_sm_metrics.successful_deployments_28_days_user,
        -- Wave 5.3
        monthly_sm_metrics.geo_enabled,
        monthly_sm_metrics.geo_nodes_all_time_event,
        monthly_sm_metrics.auto_devops_pipelines_28_days_user,
        monthly_sm_metrics.active_instance_runners_all_time_event,
        monthly_sm_metrics.active_group_runners_all_time_event,
        monthly_sm_metrics.active_project_runners_all_time_event,
        monthly_sm_metrics.gitaly_version,
        monthly_sm_metrics.gitaly_servers_all_time_event,
        -- Data Quality Flag
        monthly_sm_metrics.is_latest_data
    from monthly_sm_metrics
    left join
        billing_accounts
        on monthly_sm_metrics.dim_billing_account_id
        = billing_accounts.dim_billing_account_id
    left join
        location_country
        on monthly_sm_metrics.dim_location_country_id
        = location_country.dim_location_country_id
    left join
        subscriptions
        on subscriptions.dim_subscription_id = monthly_sm_metrics.dim_subscription_id
    left join
        most_recent_subscription_version
        on subscriptions.subscription_name
        = most_recent_subscription_version.subscription_name

),
saas_paid_user_metrics as (

    select
        monthly_saas_metrics.snapshot_month,
        monthly_saas_metrics.dim_subscription_id,
        monthly_saas_metrics.dim_namespace_id::varchar as dim_namespace_id,
        namespaces.namespace_name,
        null as uuid,
        null as hostname,
        {{ get_keyed_nulls("billing_accounts.dim_billing_account_id") }}
        as dim_billing_account_id,
        {{ get_keyed_nulls("billing_accounts.dim_crm_account_id") }}
        as dim_crm_account_id,
        monthly_saas_metrics.dim_subscription_id_original,
        subscriptions.subscription_name,
        subscriptions.subscription_status,
        most_recent_subscription_version.subscription_status
        as subscription_status_most_recent_version,
        subscriptions.term_start_date,
        subscriptions.term_end_date,
        most_recent_subscription_version.subscription_start_date,
        most_recent_subscription_version.subscription_end_date,
        monthly_saas_metrics.snapshot_date_id,
        monthly_saas_metrics.ping_created_at,
        null as dim_usage_ping_id,
        monthly_saas_metrics.instance_type as instance_type,
        null as cleaned_version,
        null as country_name,
        null as iso_2_country_code,
        null as iso_3_country_code,
        'SaaS' as delivery_type,
        -- Wave 1
        monthly_saas_metrics.license_utilization,
        monthly_saas_metrics.billable_user_count,
        null as active_user_count,
        monthly_saas_metrics.max_historical_user_count,
        monthly_saas_metrics.subscription_seats,
        -- Wave 2 & 3
        monthly_saas_metrics.umau_28_days_user,
        monthly_saas_metrics.action_monthly_active_users_project_repo_28_days_user,
        monthly_saas_metrics.merge_requests_28_days_user,
        monthly_saas_metrics.projects_with_repositories_enabled_28_days_user,
        monthly_saas_metrics.commit_comment_all_time_event,
        monthly_saas_metrics.source_code_pushes_all_time_event,
        monthly_saas_metrics.ci_pipelines_28_days_user,
        monthly_saas_metrics.ci_internal_pipelines_28_days_user,
        monthly_saas_metrics.ci_builds_28_days_user,
        monthly_saas_metrics.ci_builds_all_time_user,
        monthly_saas_metrics.ci_builds_all_time_event,
        monthly_saas_metrics.ci_runners_all_time_event,
        monthly_saas_metrics.auto_devops_enabled_all_time_event,
        monthly_saas_metrics.gitlab_shared_runners_enabled,
        monthly_saas_metrics.container_registry_enabled,
        monthly_saas_metrics.template_repositories_all_time_event,
        monthly_saas_metrics.ci_pipeline_config_repository_28_days_user,
        monthly_saas_metrics.user_unique_users_all_secure_scanners_28_days_user,
        monthly_saas_metrics.user_sast_jobs_28_days_user,
        monthly_saas_metrics.user_dast_jobs_28_days_user,
        monthly_saas_metrics.user_dependency_scanning_jobs_28_days_user,
        monthly_saas_metrics.user_license_management_jobs_28_days_user,
        monthly_saas_metrics.user_secret_detection_jobs_28_days_user,
        monthly_saas_metrics.user_container_scanning_jobs_28_days_user,
        monthly_saas_metrics.object_store_packages_enabled,
        monthly_saas_metrics.projects_with_packages_all_time_event,
        monthly_saas_metrics.projects_with_packages_28_days_user,
        monthly_saas_metrics.deployments_28_days_user,
        monthly_saas_metrics.releases_28_days_user,
        monthly_saas_metrics.epics_28_days_user,
        monthly_saas_metrics.issues_28_days_user,
        -- Wave 3.1
        monthly_saas_metrics.ci_internal_pipelines_all_time_event,
        monthly_saas_metrics.ci_external_pipelines_all_time_event,
        monthly_saas_metrics.merge_requests_all_time_event,
        monthly_saas_metrics.todos_all_time_event,
        monthly_saas_metrics.epics_all_time_event,
        monthly_saas_metrics.issues_all_time_event,
        monthly_saas_metrics.projects_all_time_event,
        monthly_saas_metrics.deployments_28_days_event,
        monthly_saas_metrics.packages_28_days_event,
        monthly_saas_metrics.sast_jobs_all_time_event,
        monthly_saas_metrics.dast_jobs_all_time_event,
        monthly_saas_metrics.dependency_scanning_jobs_all_time_event,
        monthly_saas_metrics.license_management_jobs_all_time_event,
        monthly_saas_metrics.secret_detection_jobs_all_time_event,
        monthly_saas_metrics.container_scanning_jobs_all_time_event,
        monthly_saas_metrics.projects_jenkins_active_all_time_event,
        monthly_saas_metrics.projects_bamboo_active_all_time_event,
        monthly_saas_metrics.projects_jira_active_all_time_event,
        monthly_saas_metrics.projects_drone_ci_active_all_time_event,
        monthly_saas_metrics.projects_github_active_all_time_event,
        monthly_saas_metrics.projects_jira_server_active_all_time_event,
        monthly_saas_metrics.projects_jira_dvcs_cloud_active_all_time_event,
        monthly_saas_metrics.projects_with_repositories_enabled_all_time_event,
        monthly_saas_metrics.protected_branches_all_time_event,
        monthly_saas_metrics.remote_mirrors_all_time_event,
        monthly_saas_metrics.projects_enforcing_code_owner_approval_28_days_user,
        monthly_saas_metrics.project_clusters_enabled_28_days_user,
        monthly_saas_metrics.analytics_28_days_user,
        monthly_saas_metrics.issues_edit_28_days_user,
        monthly_saas_metrics.user_packages_28_days_user,
        monthly_saas_metrics.terraform_state_api_28_days_user,
        monthly_saas_metrics.incident_management_28_days_user,
        -- Wave 3.2
        monthly_saas_metrics.auto_devops_enabled,
        monthly_saas_metrics.gitaly_clusters_instance,
        monthly_saas_metrics.epics_deepest_relationship_level_instance,
        monthly_saas_metrics.clusters_applications_cilium_all_time_event,
        monthly_saas_metrics.network_policy_forwards_all_time_event,
        monthly_saas_metrics.network_policy_drops_all_time_event,
        monthly_saas_metrics.requirements_with_test_report_all_time_event,
        monthly_saas_metrics.requirement_test_reports_ci_all_time_event,
        monthly_saas_metrics.projects_imported_from_github_all_time_event,
        monthly_saas_metrics.projects_jira_cloud_active_all_time_event,
        monthly_saas_metrics.projects_jira_dvcs_server_active_all_time_event,
        monthly_saas_metrics.service_desk_issues_all_time_event,
        monthly_saas_metrics.ci_pipelines_all_time_user,
        monthly_saas_metrics.service_desk_issues_28_days_user,
        monthly_saas_metrics.projects_jira_active_28_days_user,
        monthly_saas_metrics.projects_jira_dvcs_cloud_active_28_days_user,
        monthly_saas_metrics.projects_jira_dvcs_server_active_28_days_user,
        monthly_saas_metrics.merge_requests_with_required_code_owners_28_days_user,
        monthly_saas_metrics.analytics_value_stream_28_days_event,
        monthly_saas_metrics.code_review_user_approve_mr_28_days_user,
        monthly_saas_metrics.epics_usage_28_days_user,
        monthly_saas_metrics.ci_templates_usage_28_days_event,
        monthly_saas_metrics.project_management_issue_milestone_changed_28_days_user,
        monthly_saas_metrics.project_management_issue_iteration_changed_28_days_user,
        -- Wave 5.1
        monthly_saas_metrics.protected_branches_28_days_user,
        monthly_saas_metrics.ci_cd_lead_time_usage_28_days_event,
        monthly_saas_metrics.ci_cd_deployment_frequency_usage_28_days_event,
        monthly_saas_metrics.projects_with_repositories_enabled_all_time_user,
        monthly_saas_metrics.api_fuzzing_jobs_usage_28_days_user,
        monthly_saas_metrics.coverage_fuzzing_pipeline_usage_28_days_event,
        monthly_saas_metrics.api_fuzzing_pipeline_usage_28_days_event,
        monthly_saas_metrics.container_scanning_pipeline_usage_28_days_event,
        monthly_saas_metrics.dependency_scanning_pipeline_usage_28_days_event,
        monthly_saas_metrics.sast_pipeline_usage_28_days_event,
        monthly_saas_metrics.secret_detection_pipeline_usage_28_days_event,
        monthly_saas_metrics.dast_pipeline_usage_28_days_event,
        monthly_saas_metrics.coverage_fuzzing_jobs_28_days_user,
        monthly_saas_metrics.environments_all_time_event,
        monthly_saas_metrics.feature_flags_all_time_event,
        monthly_saas_metrics.successful_deployments_28_days_event,
        monthly_saas_metrics.failed_deployments_28_days_event,
        monthly_saas_metrics.projects_compliance_framework_all_time_event,
        monthly_saas_metrics.commit_ci_config_file_28_days_user,
        monthly_saas_metrics.view_audit_all_time_user,
        -- Wave 5.2
        monthly_saas_metrics.dependency_scanning_jobs_all_time_user,
        monthly_saas_metrics.analytics_devops_adoption_all_time_user,
        monthly_saas_metrics.projects_imported_all_time_event,
        monthly_saas_metrics.preferences_security_dashboard_28_days_user,
        monthly_saas_metrics.web_ide_edit_28_days_user,
        monthly_saas_metrics.auto_devops_pipelines_all_time_event,
        monthly_saas_metrics.projects_prometheus_active_all_time_event,
        monthly_saas_metrics.prometheus_enabled,
        monthly_saas_metrics.prometheus_metrics_enabled,
        monthly_saas_metrics.group_saml_enabled,
        monthly_saas_metrics.jira_issue_imports_all_time_event,
        monthly_saas_metrics.author_epic_all_time_user,
        monthly_saas_metrics.author_issue_all_time_user,
        monthly_saas_metrics.failed_deployments_28_days_user,
        monthly_saas_metrics.successful_deployments_28_days_user,
        -- Wave 5.3
        monthly_saas_metrics.geo_enabled,
        monthly_saas_metrics.geo_nodes_all_time_event,
        monthly_saas_metrics.auto_devops_pipelines_28_days_user,
        monthly_saas_metrics.active_instance_runners_all_time_event,
        monthly_saas_metrics.active_group_runners_all_time_event,
        monthly_saas_metrics.active_project_runners_all_time_event,
        monthly_saas_metrics.gitaly_version,
        monthly_saas_metrics.gitaly_servers_all_time_event,
        -- Data Quality Flag
        monthly_saas_metrics.is_latest_data
    from monthly_saas_metrics
    left join
        billing_accounts
        on monthly_saas_metrics.dim_billing_account_id
        = billing_accounts.dim_billing_account_id
    left join
        subscriptions
        on subscriptions.dim_subscription_id = monthly_saas_metrics.dim_subscription_id
    left join
        most_recent_subscription_version
        on subscriptions.subscription_name
        = most_recent_subscription_version.subscription_name
    left join
        namespaces
        on namespaces.dim_namespace_id = monthly_saas_metrics.dim_namespace_id

),
unioned as (

    select *
    from sm_paid_user_metrics

    union all

    select *
    from saas_paid_user_metrics

),
final as (

    select
        unioned.*,
        {{
            dbt_utils.surrogate_key(
                [
                    "snapshot_month",
                    "dim_subscription_id",
                    "delivery_type",
                    "uuid",
                    "hostname",
                    "dim_namespace_id",
                ]
            )
        }} as primary_key,
        zuora_licenses_per_subscription.zuora_licenses
    from unioned
    left join
        zuora_licenses_per_subscription
        on zuora_licenses_per_subscription.dim_subscription_id_original
        = unioned.dim_subscription_id_original
        and zuora_licenses_per_subscription.month = unioned.snapshot_month

)

{{
    dbt_audit(
        cte_ref="final",
        created_by="@mdrussell",
        updated_by="@mdrussell",
        created_date="2022-01-14",
        updated_date="2022-04-26",
    )
}}

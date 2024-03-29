{{ config(tags=["mnpi_exception"]) }}

{{ config({"schema": "common_mart_product"}) }}

{{
    simple_cte(
        [
            ("monthly_metrics", "fct_product_usage_wave_1_3_metrics_monthly"),
            ("dim_date", "dim_date"),
            ("subscriptions", "dim_subscription_snapshot_bottom_up"),
        ]
    )
}},
original_subscription_dates as (

    select distinct dim_subscription_id, subscription_start_date, subscription_end_date
    from subscriptions
    where subscription_version = 1

),
months as (select distinct first_day_of_month, days_in_month_count from dim_date),
ping_ranges as (

    select distinct
        dim_subscription_id,
        uuid,
        hostname,
        {{ usage_ping_month_range("commit_comment_all_time_event") }},
        {{ usage_ping_month_range("source_code_pushes_all_time_event") }},
        {{ usage_ping_month_range("ci_builds_all_time_event") }},
        {{ usage_ping_month_range("ci_runners_all_time_event") }},
        {{ usage_ping_month_range("template_repositories_all_time_event") }},
        {{ usage_ping_month_range("projects_with_packages_all_time_event") }},
        {{ usage_ping_month_range("auto_devops_enabled_all_time_event") }},
        {{ usage_ping_month_range("ci_internal_pipelines_all_time_event") }},
        {{ usage_ping_month_range("ci_external_pipelines_all_time_event") }},
        {{ usage_ping_month_range("merge_requests_all_time_event") }},
        {{ usage_ping_month_range("todos_all_time_event") }},
        {{ usage_ping_month_range("epics_all_time_event") }},
        {{ usage_ping_month_range("issues_all_time_event") }},
        {{ usage_ping_month_range("projects_all_time_event") }},
        {{ usage_ping_month_range("sast_jobs_all_time_event") }},
        {{ usage_ping_month_range("dast_jobs_all_time_event") }},
        {{ usage_ping_month_range("dependency_scanning_jobs_all_time_event") }},
        {{ usage_ping_month_range("license_management_jobs_all_time_event") }},
        {{ usage_ping_month_range("secret_detection_jobs_all_time_event") }},
        {{ usage_ping_month_range("container_scanning_jobs_all_time_event") }},
        {{ usage_ping_month_range("projects_jenkins_active_all_time_event") }},
        {{ usage_ping_month_range("projects_bamboo_active_all_time_event") }},
        {{ usage_ping_month_range("projects_jira_active_all_time_event") }},
        {{ usage_ping_month_range("projects_drone_ci_active_all_time_event") }},
        {{ usage_ping_month_range("projects_github_active_all_time_event") }},
        {{ usage_ping_month_range("projects_jira_server_active_all_time_event") }},
        {{ usage_ping_month_range("projects_jira_dvcs_cloud_active_all_time_event") }},
        {{
            usage_ping_month_range(
                "projects_with_repositories_enabled_all_time_event"
            )
        }},
        {{ usage_ping_month_range("protected_branches_all_time_event") }},
        {{ usage_ping_month_range("remote_mirrors_all_time_event") }},
        {{ usage_ping_month_range("clusters_applications_cilium_all_time_event") }},
        {{ usage_ping_month_range("network_policy_forwards_all_time_event") }},
        {{ usage_ping_month_range("network_policy_drops_all_time_event") }},
        {{ usage_ping_month_range("requirements_with_test_report_all_time_event") }},
        {{ usage_ping_month_range("requirement_test_reports_ci_all_time_event") }},
        {{ usage_ping_month_range("projects_imported_from_github_all_time_event") }},
        {{ usage_ping_month_range("projects_jira_cloud_active_all_time_event") }},
        {{ usage_ping_month_range("projects_jira_dvcs_server_active_all_time_event") }},
        {{ usage_ping_month_range("service_desk_issues_all_time_event") }},
        {{ usage_ping_month_range("protected_branches_28_days_user") }},
        {{ usage_ping_month_range("ci_cd_lead_time_usage_28_days_event") }},
        {{ usage_ping_month_range("ci_cd_deployment_frequency_usage_28_days_event") }},
        {{ usage_ping_month_range("projects_with_repositories_enabled_all_time_user") }},
        {{ usage_ping_month_range("api_fuzzing_jobs_usage_28_days_user") }},
        {{ usage_ping_month_range("coverage_fuzzing_pipeline_usage_28_days_event") }},
        {{ usage_ping_month_range("api_fuzzing_pipeline_usage_28_days_event") }},
        {{ usage_ping_month_range("container_scanning_pipeline_usage_28_days_event") }},
        {{ usage_ping_month_range("dependency_scanning_pipeline_usage_28_days_event") }},
        {{ usage_ping_month_range("sast_pipeline_usage_28_days_event") }},
        {{ usage_ping_month_range("secret_detection_pipeline_usage_28_days_event") }},
        {{ usage_ping_month_range("dast_pipeline_usage_28_days_event") }},
        {{ usage_ping_month_range("coverage_fuzzing_jobs_28_days_user") }},
        {{ usage_ping_month_range("environments_all_time_event") }},
        {{ usage_ping_month_range("feature_flags_all_time_event") }},
        {{ usage_ping_month_range("successful_deployments_28_days_event") }},
        {{ usage_ping_month_range("failed_deployments_28_days_event") }},
        {{ usage_ping_month_range("projects_compliance_framework_all_time_event") }},
        {{ usage_ping_month_range("commit_ci_config_file_28_days_user") }},
        {{ usage_ping_month_range("view_audit_all_time_user") }},
        {{ usage_ping_month_range("dependency_scanning_jobs_all_time_user") }},
        {{ usage_ping_month_range("analytics_devops_adoption_all_time_user") }},
        {{ usage_ping_month_range("projects_imported_all_time_event") }},
        {{ usage_ping_month_range("preferences_security_dashboard_28_days_user") }},
        {{ usage_ping_month_range("web_ide_edit_28_days_user") }},
        {{ usage_ping_month_range("auto_devops_pipelines_all_time_event") }},
        {{ usage_ping_month_range("projects_prometheus_active_all_time_event") }},
        {{ usage_ping_month_range("jira_issue_imports_all_time_event") }},
        {{ usage_ping_month_range("author_epic_all_time_user") }},
        {{ usage_ping_month_range("author_issue_all_time_user") }},
        {{ usage_ping_month_range("failed_deployments_28_days_user") }},
        {{ usage_ping_month_range("successful_deployments_28_days_user") }},
        {{ usage_ping_month_range("geo_nodes_all_time_event") }},
        {{ usage_ping_month_range("auto_devops_pipelines_28_days_user") }},
        {{ usage_ping_month_range("active_instance_runners_all_time_event") }},
        {{ usage_ping_month_range("active_group_runners_all_time_event") }},
        {{ usage_ping_month_range("active_project_runners_all_time_event") }},
        {{ usage_ping_month_range("gitaly_servers_all_time_event") }}
    from monthly_metrics

),
diffs as (

    select
        dim_subscription_id,
        dim_subscription_id_original,
        dim_billing_account_id,
        snapshot_month,
        uuid,
        hostname,
        ping_created_at,
        ping_created_at::date - lag(ping_created_at::date)
        ignore nulls over (
            partition by dim_subscription_id, uuid, hostname order by snapshot_month
        ) as date_diff,
        iff(date_diff > 0, date_diff, 1) as days_since_last_ping,
        {{ usage_ping_over_ping_difference("commit_comment_all_time_event") }},
        {{ usage_ping_over_ping_difference("source_code_pushes_all_time_event") }},
        {{ usage_ping_over_ping_difference("ci_builds_all_time_event") }},
        {{ usage_ping_over_ping_difference("ci_runners_all_time_event") }},
        {{ usage_ping_over_ping_difference("template_repositories_all_time_event") }},
        {{ usage_ping_over_ping_difference("projects_with_packages_all_time_event") }},
        {{ usage_ping_over_ping_difference("auto_devops_enabled_all_time_event") }},
        {{ usage_ping_over_ping_difference("ci_internal_pipelines_all_time_event") }},
        {{ usage_ping_over_ping_difference("ci_external_pipelines_all_time_event") }},
        {{ usage_ping_over_ping_difference("merge_requests_all_time_event") }},
        {{ usage_ping_over_ping_difference("todos_all_time_event") }},
        {{ usage_ping_over_ping_difference("epics_all_time_event") }},
        {{ usage_ping_over_ping_difference("issues_all_time_event") }},
        {{ usage_ping_over_ping_difference("projects_all_time_event") }},
        {{ usage_ping_over_ping_difference("sast_jobs_all_time_event") }},
        {{ usage_ping_over_ping_difference("dast_jobs_all_time_event") }},
        {{ usage_ping_over_ping_difference("dependency_scanning_jobs_all_time_event") }},
        {{ usage_ping_over_ping_difference("license_management_jobs_all_time_event") }},
        {{ usage_ping_over_ping_difference("secret_detection_jobs_all_time_event") }},
        {{ usage_ping_over_ping_difference("container_scanning_jobs_all_time_event") }},
        {{ usage_ping_over_ping_difference("projects_jenkins_active_all_time_event") }},
        {{ usage_ping_over_ping_difference("projects_bamboo_active_all_time_event") }},
        {{ usage_ping_over_ping_difference("projects_jira_active_all_time_event") }},
        {{ usage_ping_over_ping_difference("projects_drone_ci_active_all_time_event") }},
        {{ usage_ping_over_ping_difference("projects_github_active_all_time_event") }},
        {{
            usage_ping_over_ping_difference(
                "projects_jira_server_active_all_time_event"
            )
        }},
        {{
            usage_ping_over_ping_difference(
                "projects_jira_dvcs_cloud_active_all_time_event"
            )
        }},
        {{
            usage_ping_over_ping_difference(
                "projects_with_repositories_enabled_all_time_event"
            )
        }},
        {{ usage_ping_over_ping_difference("protected_branches_all_time_event") }},
        {{ usage_ping_over_ping_difference("remote_mirrors_all_time_event") }},
        {{
            usage_ping_over_ping_difference(
                "clusters_applications_cilium_all_time_event"
            )
        }},
        {{ usage_ping_over_ping_difference("network_policy_forwards_all_time_event") }},
        {{ usage_ping_over_ping_difference("network_policy_drops_all_time_event") }},
        {{
            usage_ping_over_ping_difference(
                "requirements_with_test_report_all_time_event"
            )
        }},
        {{
            usage_ping_over_ping_difference(
                "requirement_test_reports_ci_all_time_event"
            )
        }},
        {{
            usage_ping_over_ping_difference(
                "projects_imported_from_github_all_time_event"
            )
        }},
        {{
            usage_ping_over_ping_difference(
                "projects_jira_cloud_active_all_time_event"
            )
        }},
        {{
            usage_ping_over_ping_difference(
                "projects_jira_dvcs_server_active_all_time_event"
            )
        }},
        {{ usage_ping_over_ping_difference("service_desk_issues_all_time_event") }},
        {{ usage_ping_over_ping_difference("protected_branches_28_days_user") }},
        {{ usage_ping_over_ping_difference("ci_cd_lead_time_usage_28_days_event") }},
        {{
            usage_ping_over_ping_difference(
                "ci_cd_deployment_frequency_usage_28_days_event"
            )
        }},
        {{
            usage_ping_over_ping_difference(
                "projects_with_repositories_enabled_all_time_user"
            )
        }},
        {{ usage_ping_over_ping_difference("api_fuzzing_jobs_usage_28_days_user") }},
        {{
            usage_ping_over_ping_difference(
                "coverage_fuzzing_pipeline_usage_28_days_event"
            )
        }},
        {{
            usage_ping_over_ping_difference(
                "api_fuzzing_pipeline_usage_28_days_event"
            )
        }},
        {{
            usage_ping_over_ping_difference(
                "container_scanning_pipeline_usage_28_days_event"
            )
        }},
        {{
            usage_ping_over_ping_difference(
                "dependency_scanning_pipeline_usage_28_days_event"
            )
        }},
        {{ usage_ping_over_ping_difference("sast_pipeline_usage_28_days_event") }},
        {{
            usage_ping_over_ping_difference(
                "secret_detection_pipeline_usage_28_days_event"
            )
        }},
        {{ usage_ping_over_ping_difference("dast_pipeline_usage_28_days_event") }},
        {{ usage_ping_over_ping_difference("coverage_fuzzing_jobs_28_days_user") }},
        {{ usage_ping_over_ping_difference("environments_all_time_event") }},
        {{ usage_ping_over_ping_difference("feature_flags_all_time_event") }},
        {{ usage_ping_over_ping_difference("successful_deployments_28_days_event") }},
        {{ usage_ping_over_ping_difference("failed_deployments_28_days_event") }},
        {{
            usage_ping_over_ping_difference(
                "projects_compliance_framework_all_time_event"
            )
        }},
        {{ usage_ping_over_ping_difference("commit_ci_config_file_28_days_user") }},
        {{ usage_ping_over_ping_difference("view_audit_all_time_user") }},
        {{ usage_ping_over_ping_difference("dependency_scanning_jobs_all_time_user") }},
        {{ usage_ping_over_ping_difference("analytics_devops_adoption_all_time_user") }},
        {{ usage_ping_over_ping_difference("projects_imported_all_time_event") }},
        {{
            usage_ping_over_ping_difference(
                "preferences_security_dashboard_28_days_user"
            )
        }},
        {{ usage_ping_over_ping_difference("web_ide_edit_28_days_user") }},
        {{ usage_ping_over_ping_difference("auto_devops_pipelines_all_time_event") }},
        {{
            usage_ping_over_ping_difference(
                "projects_prometheus_active_all_time_event"
            )
        }},
        {{ usage_ping_over_ping_difference("jira_issue_imports_all_time_event") }},
        {{ usage_ping_over_ping_difference("author_epic_all_time_user") }},
        {{ usage_ping_over_ping_difference("author_issue_all_time_user") }},
        {{ usage_ping_over_ping_difference("failed_deployments_28_days_user") }},
        {{ usage_ping_over_ping_difference("successful_deployments_28_days_user") }},
        {{ usage_ping_over_ping_difference("geo_nodes_all_time_event") }},
        {{ usage_ping_over_ping_difference("auto_devops_pipelines_28_days_user") }},
        {{ usage_ping_over_ping_difference("active_instance_runners_all_time_event") }},
        {{ usage_ping_over_ping_difference("active_group_runners_all_time_event") }},
        {{ usage_ping_over_ping_difference("active_project_runners_all_time_event") }},
        {{ usage_ping_over_ping_difference("gitaly_servers_all_time_event") }}
    from monthly_metrics

),
smoothed_diffs as (

    select
        dim_subscription_id,
        dim_subscription_id_original,
        dim_billing_account_id,
        snapshot_month,
        uuid,
        hostname,
        ping_created_at,
        days_since_last_ping,
        months.days_in_month_count,
        {{ usage_ping_over_ping_smoothed("commit_comment_all_time_event") }},
        {{ usage_ping_over_ping_smoothed("source_code_pushes_all_time_event") }},
        {{ usage_ping_over_ping_smoothed("ci_builds_all_time_event") }},
        {{ usage_ping_over_ping_smoothed("ci_runners_all_time_event") }},
        {{ usage_ping_over_ping_smoothed("template_repositories_all_time_event") }},
        {{ usage_ping_over_ping_smoothed("projects_with_packages_all_time_event") }},
        {{ usage_ping_over_ping_smoothed("auto_devops_enabled_all_time_event") }},
        {{ usage_ping_over_ping_smoothed("ci_internal_pipelines_all_time_event") }},
        {{ usage_ping_over_ping_smoothed("ci_external_pipelines_all_time_event") }},
        {{ usage_ping_over_ping_smoothed("merge_requests_all_time_event") }},
        {{ usage_ping_over_ping_smoothed("todos_all_time_event") }},
        {{ usage_ping_over_ping_smoothed("epics_all_time_event") }},
        {{ usage_ping_over_ping_smoothed("issues_all_time_event") }},
        {{ usage_ping_over_ping_smoothed("projects_all_time_event") }},
        {{ usage_ping_over_ping_smoothed("sast_jobs_all_time_event") }},
        {{ usage_ping_over_ping_smoothed("dast_jobs_all_time_event") }},
        {{ usage_ping_over_ping_smoothed("dependency_scanning_jobs_all_time_event") }},
        {{ usage_ping_over_ping_smoothed("license_management_jobs_all_time_event") }},
        {{ usage_ping_over_ping_smoothed("secret_detection_jobs_all_time_event") }},
        {{ usage_ping_over_ping_smoothed("container_scanning_jobs_all_time_event") }},
        {{ usage_ping_over_ping_smoothed("projects_jenkins_active_all_time_event") }},
        {{ usage_ping_over_ping_smoothed("projects_bamboo_active_all_time_event") }},
        {{ usage_ping_over_ping_smoothed("projects_jira_active_all_time_event") }},
        {{ usage_ping_over_ping_smoothed("projects_drone_ci_active_all_time_event") }},
        {{ usage_ping_over_ping_smoothed("projects_github_active_all_time_event") }},
        {{
            usage_ping_over_ping_smoothed(
                "projects_jira_server_active_all_time_event"
            )
        }},
        {{
            usage_ping_over_ping_smoothed(
                "projects_jira_dvcs_cloud_active_all_time_event"
            )
        }},
        {{
            usage_ping_over_ping_smoothed(
                "projects_with_repositories_enabled_all_time_event"
            )
        }},
        {{ usage_ping_over_ping_smoothed("protected_branches_all_time_event") }},
        {{ usage_ping_over_ping_smoothed("remote_mirrors_all_time_event") }},
        {{
            usage_ping_over_ping_smoothed(
                "clusters_applications_cilium_all_time_event"
            )
        }},
        {{ usage_ping_over_ping_smoothed("network_policy_forwards_all_time_event") }},
        {{ usage_ping_over_ping_smoothed("network_policy_drops_all_time_event") }},
        {{
            usage_ping_over_ping_smoothed(
                "requirements_with_test_report_all_time_event"
            )
        }},
        {{
            usage_ping_over_ping_smoothed(
                "requirement_test_reports_ci_all_time_event"
            )
        }},
        {{
            usage_ping_over_ping_smoothed(
                "projects_imported_from_github_all_time_event"
            )
        }},
        {{ usage_ping_over_ping_smoothed("projects_jira_cloud_active_all_time_event") }},
        {{
            usage_ping_over_ping_smoothed(
                "projects_jira_dvcs_server_active_all_time_event"
            )
        }},
        {{ usage_ping_over_ping_smoothed("service_desk_issues_all_time_event") }},
        {{ usage_ping_over_ping_smoothed("protected_branches_28_days_user") }},
        {{ usage_ping_over_ping_smoothed("ci_cd_lead_time_usage_28_days_event") }},
        {{
            usage_ping_over_ping_smoothed(
                "ci_cd_deployment_frequency_usage_28_days_event"
            )
        }},
        {{
            usage_ping_over_ping_smoothed(
                "projects_with_repositories_enabled_all_time_user"
            )
        }},
        {{ usage_ping_over_ping_smoothed("api_fuzzing_jobs_usage_28_days_user") }},
        {{
            usage_ping_over_ping_smoothed(
                "coverage_fuzzing_pipeline_usage_28_days_event"
            )
        }},
        {{ usage_ping_over_ping_smoothed("api_fuzzing_pipeline_usage_28_days_event") }},
        {{
            usage_ping_over_ping_smoothed(
                "container_scanning_pipeline_usage_28_days_event"
            )
        }},
        {{
            usage_ping_over_ping_smoothed(
                "dependency_scanning_pipeline_usage_28_days_event"
            )
        }},
        {{ usage_ping_over_ping_smoothed("sast_pipeline_usage_28_days_event") }},
        {{
            usage_ping_over_ping_smoothed(
                "secret_detection_pipeline_usage_28_days_event"
            )
        }},
        {{ usage_ping_over_ping_smoothed("dast_pipeline_usage_28_days_event") }},
        {{ usage_ping_over_ping_smoothed("coverage_fuzzing_jobs_28_days_user") }},
        {{ usage_ping_over_ping_smoothed("environments_all_time_event") }},
        {{ usage_ping_over_ping_smoothed("feature_flags_all_time_event") }},
        {{ usage_ping_over_ping_smoothed("successful_deployments_28_days_event") }},
        {{ usage_ping_over_ping_smoothed("failed_deployments_28_days_event") }},
        {{
            usage_ping_over_ping_smoothed(
                "projects_compliance_framework_all_time_event"
            )
        }},
        {{ usage_ping_over_ping_smoothed("commit_ci_config_file_28_days_user") }},
        {{ usage_ping_over_ping_smoothed("view_audit_all_time_user") }},
        {{ usage_ping_over_ping_smoothed("dependency_scanning_jobs_all_time_user") }},
        {{ usage_ping_over_ping_smoothed("analytics_devops_adoption_all_time_user") }},
        {{ usage_ping_over_ping_smoothed("projects_imported_all_time_event") }},
        {{
            usage_ping_over_ping_smoothed(
                "preferences_security_dashboard_28_days_user"
            )
        }},
        {{ usage_ping_over_ping_smoothed("web_ide_edit_28_days_user") }},
        {{ usage_ping_over_ping_smoothed("auto_devops_pipelines_all_time_event") }},
        {{ usage_ping_over_ping_smoothed("projects_prometheus_active_all_time_event") }},
        {{ usage_ping_over_ping_smoothed("jira_issue_imports_all_time_event") }},
        {{ usage_ping_over_ping_smoothed("author_epic_all_time_user") }},
        {{ usage_ping_over_ping_smoothed("author_issue_all_time_user") }},
        {{ usage_ping_over_ping_smoothed("failed_deployments_28_days_user") }},
        {{ usage_ping_over_ping_smoothed("successful_deployments_28_days_user") }},
        {{ usage_ping_over_ping_smoothed("geo_nodes_all_time_event") }},
        {{ usage_ping_over_ping_smoothed("auto_devops_pipelines_28_days_user") }},
        {{ usage_ping_over_ping_smoothed("active_instance_runners_all_time_event") }},
        {{ usage_ping_over_ping_smoothed("active_group_runners_all_time_event") }},
        {{ usage_ping_over_ping_smoothed("active_project_runners_all_time_event") }},
        {{ usage_ping_over_ping_smoothed("gitaly_servers_all_time_event") }}
    from diffs
    inner join months on diffs.snapshot_month = months.first_day_of_month

),
final as (

    select
        smoothed_diffs.dim_subscription_id,
        smoothed_diffs.dim_subscription_id_original,
        smoothed_diffs.dim_billing_account_id,
        subscriptions.subscription_status,
        subscriptions.subscription_start_date,
        subscriptions.subscription_end_date,
        subscriptions_original.subscription_status as subscription_status_original,
        original_subscription_dates.subscription_start_date
        as subscription_start_date_original,
        original_subscription_dates.subscription_end_date
        as subscription_end_date_original,
        smoothed_diffs.snapshot_month,
        smoothed_diffs.uuid,
        smoothed_diffs.hostname,
        {{ usage_ping_over_ping_estimated("commit_comment_all_time_event") }},
        {{ usage_ping_over_ping_estimated("source_code_pushes_all_time_event") }},
        {{ usage_ping_over_ping_estimated("ci_builds_all_time_event") }},
        {{ usage_ping_over_ping_estimated("ci_runners_all_time_event") }},
        {{ usage_ping_over_ping_estimated("template_repositories_all_time_event") }},
        {{ usage_ping_over_ping_estimated("projects_with_packages_all_time_event") }},
        {{ usage_ping_over_ping_estimated("auto_devops_enabled_all_time_event") }},
        {{ usage_ping_over_ping_estimated("ci_internal_pipelines_all_time_event") }},
        {{ usage_ping_over_ping_estimated("ci_external_pipelines_all_time_event") }},
        {{ usage_ping_over_ping_estimated("merge_requests_all_time_event") }},
        {{ usage_ping_over_ping_estimated("todos_all_time_event") }},
        {{ usage_ping_over_ping_estimated("epics_all_time_event") }},
        {{ usage_ping_over_ping_estimated("issues_all_time_event") }},
        {{ usage_ping_over_ping_estimated("projects_all_time_event") }},
        {{ usage_ping_over_ping_estimated("sast_jobs_all_time_event") }},
        {{ usage_ping_over_ping_estimated("dast_jobs_all_time_event") }},
        {{ usage_ping_over_ping_estimated("dependency_scanning_jobs_all_time_event") }},
        {{ usage_ping_over_ping_estimated("license_management_jobs_all_time_event") }},
        {{ usage_ping_over_ping_estimated("secret_detection_jobs_all_time_event") }},
        {{ usage_ping_over_ping_estimated("container_scanning_jobs_all_time_event") }},
        {{ usage_ping_over_ping_estimated("projects_jenkins_active_all_time_event") }},
        {{ usage_ping_over_ping_estimated("projects_bamboo_active_all_time_event") }},
        {{ usage_ping_over_ping_estimated("projects_jira_active_all_time_event") }},
        {{ usage_ping_over_ping_estimated("projects_drone_ci_active_all_time_event") }},
        {{ usage_ping_over_ping_estimated("projects_github_active_all_time_event") }},
        {{
            usage_ping_over_ping_estimated(
                "projects_jira_server_active_all_time_event"
            )
        }},
        {{
            usage_ping_over_ping_estimated(
                "projects_jira_dvcs_cloud_active_all_time_event"
            )
        }},
        {{
            usage_ping_over_ping_estimated(
                "projects_with_repositories_enabled_all_time_event"
            )
        }},
        {{ usage_ping_over_ping_estimated("protected_branches_all_time_event") }},
        {{ usage_ping_over_ping_estimated("remote_mirrors_all_time_event") }},
        {{
            usage_ping_over_ping_estimated(
                "clusters_applications_cilium_all_time_event"
            )
        }},
        {{ usage_ping_over_ping_estimated("network_policy_forwards_all_time_event") }},
        {{ usage_ping_over_ping_estimated("network_policy_drops_all_time_event") }},
        {{
            usage_ping_over_ping_estimated(
                "requirements_with_test_report_all_time_event"
            )
        }},
        {{
            usage_ping_over_ping_estimated(
                "requirement_test_reports_ci_all_time_event"
            )
        }},
        {{
            usage_ping_over_ping_estimated(
                "projects_imported_from_github_all_time_event"
            )
        }},
        {{
            usage_ping_over_ping_estimated(
                "projects_jira_cloud_active_all_time_event"
            )
        }},
        {{
            usage_ping_over_ping_estimated(
                "projects_jira_dvcs_server_active_all_time_event"
            )
        }},
        {{ usage_ping_over_ping_estimated("service_desk_issues_all_time_event") }},
        {{ usage_ping_over_ping_estimated("protected_branches_28_days_user") }},
        {{ usage_ping_over_ping_estimated("ci_cd_lead_time_usage_28_days_event") }},
        {{
            usage_ping_over_ping_estimated(
                "ci_cd_deployment_frequency_usage_28_days_event"
            )
        }},
        {{
            usage_ping_over_ping_estimated(
                "projects_with_repositories_enabled_all_time_user"
            )
        }},
        {{ usage_ping_over_ping_estimated("api_fuzzing_jobs_usage_28_days_user") }},
        {{
            usage_ping_over_ping_estimated(
                "coverage_fuzzing_pipeline_usage_28_days_event"
            )
        }},
        {{ usage_ping_over_ping_estimated("api_fuzzing_pipeline_usage_28_days_event") }},
        {{
            usage_ping_over_ping_estimated(
                "container_scanning_pipeline_usage_28_days_event"
            )
        }},
        {{
            usage_ping_over_ping_estimated(
                "dependency_scanning_pipeline_usage_28_days_event"
            )
        }},
        {{ usage_ping_over_ping_estimated("sast_pipeline_usage_28_days_event") }},
        {{
            usage_ping_over_ping_estimated(
                "secret_detection_pipeline_usage_28_days_event"
            )
        }},
        {{ usage_ping_over_ping_estimated("dast_pipeline_usage_28_days_event") }},
        {{ usage_ping_over_ping_estimated("coverage_fuzzing_jobs_28_days_user") }},
        {{ usage_ping_over_ping_estimated("environments_all_time_event") }},
        {{ usage_ping_over_ping_estimated("feature_flags_all_time_event") }},
        {{ usage_ping_over_ping_estimated("successful_deployments_28_days_event") }},
        {{ usage_ping_over_ping_estimated("failed_deployments_28_days_event") }},
        {{
            usage_ping_over_ping_estimated(
                "projects_compliance_framework_all_time_event"
            )
        }},
        {{ usage_ping_over_ping_estimated("commit_ci_config_file_28_days_user") }},
        {{ usage_ping_over_ping_estimated("view_audit_all_time_user") }},
        {{ usage_ping_over_ping_estimated("dependency_scanning_jobs_all_time_user") }},
        {{ usage_ping_over_ping_estimated("analytics_devops_adoption_all_time_user") }},
        {{ usage_ping_over_ping_estimated("projects_imported_all_time_event") }},
        {{
            usage_ping_over_ping_estimated(
                "preferences_security_dashboard_28_days_user"
            )
        }},
        {{ usage_ping_over_ping_estimated("web_ide_edit_28_days_user") }},
        {{ usage_ping_over_ping_estimated("auto_devops_pipelines_all_time_event") }},
        {{
            usage_ping_over_ping_estimated(
                "projects_prometheus_active_all_time_event"
            )
        }},
        {{ usage_ping_over_ping_estimated("jira_issue_imports_all_time_event") }},
        {{ usage_ping_over_ping_estimated("author_epic_all_time_user") }},
        {{ usage_ping_over_ping_estimated("author_issue_all_time_user") }},
        {{ usage_ping_over_ping_estimated("failed_deployments_28_days_user") }},
        {{ usage_ping_over_ping_estimated("successful_deployments_28_days_user") }},
        {{ usage_ping_over_ping_estimated("geo_nodes_all_time_event") }},
        {{ usage_ping_over_ping_estimated("auto_devops_pipelines_28_days_user") }},
        {{ usage_ping_over_ping_estimated("active_instance_runners_all_time_event") }},
        {{ usage_ping_over_ping_estimated("active_group_runners_all_time_event") }},
        {{ usage_ping_over_ping_estimated("active_project_runners_all_time_event") }},
        {{ usage_ping_over_ping_estimated("gitaly_servers_all_time_event") }}
    from smoothed_diffs
    left join
        ping_ranges
        on smoothed_diffs.dim_subscription_id = ping_ranges.dim_subscription_id
        and smoothed_diffs.uuid = ping_ranges.uuid
        and smoothed_diffs.hostname = ping_ranges.hostname
    left join
        subscriptions
        on smoothed_diffs.dim_subscription_id = subscriptions.dim_subscription_id
        and ifnull(
            smoothed_diffs.ping_created_at::date,
            dateadd('day', -1, smoothed_diffs.snapshot_month)
        )
        = to_date(to_char(subscriptions.snapshot_id), 'YYYYMMDD')
    left join
        subscriptions as subscriptions_original
        on smoothed_diffs.dim_subscription_id_original
        = subscriptions_original.dim_subscription_id_original
        and ifnull(
            smoothed_diffs.ping_created_at::date,
            dateadd('day', -1, smoothed_diffs.snapshot_month)
        )
        = to_date(to_char(subscriptions_original.snapshot_id), 'YYYYMMDD')
    left join
        original_subscription_dates
        on original_subscription_dates.dim_subscription_id
        = smoothed_diffs.dim_subscription_id_original

)

{{
    dbt_audit(
        cte_ref="final",
        created_by="@ischweickartDD",
        updated_by="@mdrussell",
        created_date="2021-03-04",
        updated_date="2022-04-21",
    )
}}

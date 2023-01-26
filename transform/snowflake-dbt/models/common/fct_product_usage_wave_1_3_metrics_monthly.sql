{{ config(tags=["product", "mnpi_exception"]) }}

{{
    simple_cte(
        [
            ("subscriptions", "bdg_subscription_product_rate_plan"),
            ("dates", "dim_date"),
            ("seat_link", "fct_usage_self_managed_seat_link"),
            ("smau", "fct_usage_ping_subscription_mapped_smau"),
        ]
    )
}},
sm_subscriptions as (

    select
        dim_subscription_id,
        dim_subscription_id_original,
        dim_billing_account_id,
        first_day_of_month as snapshot_month
    from subscriptions
    inner join dates on dates.date_actual between '2017-04-01' and current_date  -- first month Usage Ping was collected
    where product_delivery_type = 'Self-Managed' {{ dbt_utils.group_by(n=4) }}

),
smau_convert as (

    select distinct
        dim_subscription_id,
        uuid,
        hostname,
        snapshot_month,
        {{
            convert_variant_to_number_field(
                "manage_analytics_total_unique_counts_monthly"
            )
        }} as analytics_28_days_user,
        {{
            convert_variant_to_number_field(
                "plan_redis_hll_counters_issues_edit_issues_edit_total_unique_counts_monthly"
            )
        }}
        as issues_edit_28_days_user,
        {{
            convert_variant_to_number_field(
                "package_redis_hll_counters_user_packages_user_packages_total_unique_counts_monthly"
            )
        }}
        as user_packages_28_days_user,
        {{
            convert_variant_to_number_field(
                "configure_redis_hll_counters_terraform_p_terraform_state_api_unique_users_monthly"
            )
        }}
        as terraform_state_api_28_days_user,
        {{
            convert_variant_to_number_field(
                "monitor_incident_management_activer_user_28_days"
            )
        }} as incident_management_28_days_user
    from smau

),
usage_ping as (

    select *
    from {{ ref("prep_usage_ping_subscription_mapped_wave_2_3_metrics") }}
    where dim_subscription_id is not null and ping_source = 'Self-Managed'
    qualify
        row_number() over (
            partition by dim_subscription_id, uuid, hostname, ping_created_at_month
            order by ping_created_at desc
        )
        = 1

),
joined as (

    select
        sm_subscriptions.dim_subscription_id,
        sm_subscriptions.dim_subscription_id_original,
        sm_subscriptions.dim_billing_account_id,
        sm_subscriptions.snapshot_month,
        {{ get_date_id("sm_subscriptions.snapshot_month") }} as snapshot_date_id,
        seat_link.report_date as seat_link_report_date,
        {{ get_date_id("seat_link.report_date") }} as seat_link_report_date_id,
        usage_ping.dim_usage_ping_id,
        usage_ping.ping_created_at,
        {{ get_date_id("usage_ping.ping_created_at") }} as ping_created_date_id,
        usage_ping.uuid,
        usage_ping.hostname,
        usage_ping.instance_type,
        usage_ping.dim_license_id,
        usage_ping.license_md5,
        usage_ping.cleaned_version,
        usage_ping.dim_location_country_id,
        -- Wave 1
        div0(
            usage_ping.license_billable_users,
            ifnull(usage_ping.license_user_count, seat_link.license_user_count)
        ) as license_utilization,
        usage_ping.license_billable_users as billable_user_count,
        usage_ping.instance_user_count as active_user_count,
        ifnull(
            usage_ping.historical_max_users, seat_link.max_historical_user_count
        ) as max_historical_user_count,
        ifnull(
            usage_ping.license_user_count, seat_link.license_user_count
        ) as license_user_count,
        -- Wave 2 & 3
        usage_ping.umau_28_days_user,
        usage_ping.action_monthly_active_users_project_repo_28_days_user,
        usage_ping.merge_requests_28_days_user,
        usage_ping.projects_with_repositories_enabled_28_days_user,
        usage_ping.commit_comment_all_time_event,
        usage_ping.source_code_pushes_all_time_event,
        usage_ping.ci_pipelines_28_days_user,
        usage_ping.ci_internal_pipelines_28_days_user,
        usage_ping.ci_builds_28_days_user,
        usage_ping.ci_builds_all_time_user,
        usage_ping.ci_builds_all_time_event,
        usage_ping.ci_runners_all_time_event,
        usage_ping.auto_devops_enabled_all_time_event,
        usage_ping.gitlab_shared_runners_enabled,
        usage_ping.container_registry_enabled,
        usage_ping.template_repositories_all_time_event,
        usage_ping.ci_pipeline_config_repository_28_days_user,
        usage_ping.user_unique_users_all_secure_scanners_28_days_user,
        usage_ping.user_sast_jobs_28_days_user,
        usage_ping.user_dast_jobs_28_days_user,
        usage_ping.user_dependency_scanning_jobs_28_days_user,
        usage_ping.user_license_management_jobs_28_days_user,
        usage_ping.user_secret_detection_jobs_28_days_user,
        usage_ping.user_container_scanning_jobs_28_days_user,
        usage_ping.object_store_packages_enabled,
        usage_ping.projects_with_packages_all_time_event,
        usage_ping.projects_with_packages_28_days_user,
        usage_ping.deployments_28_days_user,
        usage_ping.releases_28_days_user,
        usage_ping.epics_28_days_user,
        usage_ping.issues_28_days_user,
        -- Wave 3.1
        usage_ping.ci_internal_pipelines_all_time_event,
        usage_ping.ci_external_pipelines_all_time_event,
        usage_ping.merge_requests_all_time_event,
        usage_ping.todos_all_time_event,
        usage_ping.epics_all_time_event,
        usage_ping.issues_all_time_event,
        usage_ping.projects_all_time_event,
        usage_ping.deployments_28_days_event,
        usage_ping.packages_28_days_event,
        usage_ping.sast_jobs_all_time_event,
        usage_ping.dast_jobs_all_time_event,
        usage_ping.dependency_scanning_jobs_all_time_event,
        usage_ping.license_management_jobs_all_time_event,
        usage_ping.secret_detection_jobs_all_time_event,
        usage_ping.container_scanning_jobs_all_time_event,
        usage_ping.projects_jenkins_active_all_time_event,
        usage_ping.projects_bamboo_active_all_time_event,
        usage_ping.projects_jira_active_all_time_event,
        usage_ping.projects_drone_ci_active_all_time_event,
        usage_ping.projects_github_active_all_time_event,
        usage_ping.projects_jira_server_active_all_time_event,
        usage_ping.projects_jira_dvcs_cloud_active_all_time_event,
        usage_ping.projects_with_repositories_enabled_all_time_event,
        usage_ping.protected_branches_all_time_event,
        usage_ping.remote_mirrors_all_time_event,
        usage_ping.projects_enforcing_code_owner_approval_28_days_user,
        usage_ping.project_clusters_enabled_28_days_user,
        {{ null_negative_numbers("smau_convert.analytics_28_days_user") }}
        as analytics_28_days_user,
        {{ null_negative_numbers("smau_convert.issues_edit_28_days_user") }}
        as issues_edit_28_days_user,
        {{ null_negative_numbers("smau_convert.user_packages_28_days_user") }}
        as user_packages_28_days_user,
        {{ null_negative_numbers("smau_convert.terraform_state_api_28_days_user") }}
        as terraform_state_api_28_days_user,
        {{ null_negative_numbers("smau_convert.incident_management_28_days_user") }}
        as incident_management_28_days_user,
        -- Wave 3.2
        usage_ping.auto_devops_enabled,
        usage_ping.gitaly_clusters_instance,
        usage_ping.epics_deepest_relationship_level_instance,
        usage_ping.clusters_applications_cilium_all_time_event,
        usage_ping.network_policy_forwards_all_time_event,
        usage_ping.network_policy_drops_all_time_event,
        usage_ping.requirements_with_test_report_all_time_event,
        usage_ping.requirement_test_reports_ci_all_time_event,
        usage_ping.projects_imported_from_github_all_time_event,
        usage_ping.projects_jira_cloud_active_all_time_event,
        usage_ping.projects_jira_dvcs_server_active_all_time_event,
        usage_ping.service_desk_issues_all_time_event,
        usage_ping.ci_pipelines_all_time_user,
        usage_ping.service_desk_issues_28_days_user,
        usage_ping.projects_jira_active_28_days_user,
        usage_ping.projects_jira_dvcs_cloud_active_28_days_user,
        usage_ping.projects_jira_dvcs_server_active_28_days_user,
        usage_ping.merge_requests_with_required_code_owners_28_days_user,
        usage_ping.analytics_value_stream_28_days_event,
        usage_ping.code_review_user_approve_mr_28_days_user,
        usage_ping.epics_usage_28_days_user,
        usage_ping.ci_templates_usage_28_days_event,
        usage_ping.project_management_issue_milestone_changed_28_days_user,
        usage_ping.project_management_issue_iteration_changed_28_days_user,
        -- Wave 5.1
        usage_ping.protected_branches_28_days_user,
        usage_ping.ci_cd_lead_time_usage_28_days_event,
        usage_ping.ci_cd_deployment_frequency_usage_28_days_event,
        usage_ping.projects_with_repositories_enabled_all_time_user,
        usage_ping.api_fuzzing_jobs_usage_28_days_user,
        usage_ping.coverage_fuzzing_pipeline_usage_28_days_event,
        usage_ping.api_fuzzing_pipeline_usage_28_days_event,
        usage_ping.container_scanning_pipeline_usage_28_days_event,
        usage_ping.dependency_scanning_pipeline_usage_28_days_event,
        usage_ping.sast_pipeline_usage_28_days_event,
        usage_ping.secret_detection_pipeline_usage_28_days_event,
        usage_ping.dast_pipeline_usage_28_days_event,
        usage_ping.coverage_fuzzing_jobs_28_days_user,
        usage_ping.environments_all_time_event,
        usage_ping.feature_flags_all_time_event,
        usage_ping.successful_deployments_28_days_event,
        usage_ping.failed_deployments_28_days_event,
        usage_ping.projects_compliance_framework_all_time_event,
        usage_ping.commit_ci_config_file_28_days_user,
        usage_ping.view_audit_all_time_user,
        -- Wave 5.2
        usage_ping.dependency_scanning_jobs_all_time_user,
        usage_ping.analytics_devops_adoption_all_time_user,
        usage_ping.projects_imported_all_time_event,
        usage_ping.preferences_security_dashboard_28_days_user,
        usage_ping.web_ide_edit_28_days_user,
        usage_ping.auto_devops_pipelines_all_time_event,
        usage_ping.projects_prometheus_active_all_time_event,
        usage_ping.prometheus_enabled,
        usage_ping.prometheus_metrics_enabled,
        usage_ping.group_saml_enabled,
        usage_ping.jira_issue_imports_all_time_event,
        usage_ping.author_epic_all_time_user,
        usage_ping.author_issue_all_time_user,
        usage_ping.failed_deployments_28_days_user,
        usage_ping.successful_deployments_28_days_user,
        -- Wave 5.3
        usage_ping.geo_enabled,
        usage_ping.geo_nodes_all_time_event,
        usage_ping.auto_devops_pipelines_28_days_user,
        usage_ping.active_instance_runners_all_time_event,
        usage_ping.active_group_runners_all_time_event,
        usage_ping.active_project_runners_all_time_event,
        usage_ping.gitaly_version,
        usage_ping.gitaly_servers_all_time_event,
        -- Data Quality Flags
        iff(
            usage_ping.instance_user_count != seat_link.active_user_count,
            usage_ping.instance_user_count,
            null
        ) as instance_user_count_not_aligned,
        iff(
            usage_ping.historical_max_users != seat_link.max_historical_user_count,
            usage_ping.historical_max_users,
            null
        ) as historical_max_users_not_aligned,
        seat_link.is_subscription_in_zuora as is_seat_link_subscription_in_zuora,
        seat_link.is_rate_plan_in_zuora as is_seat_link_rate_plan_in_zuora,
        seat_link.is_active_user_count_available
        as is_seat_link_active_user_count_available,
        usage_ping.is_license_mapped_to_subscription
        as is_usage_ping_license_mapped_to_subscription,
        usage_ping.is_license_subscription_id_valid
        as is_usage_ping_license_subscription_id_valid,
        iff(
            usage_ping.ping_created_at is not null or seat_link.report_date is not null,
            true,
            false
        ) as is_data_in_subscription_month,
        iff(
            is_data_in_subscription_month = true
            and row_number() over (
                partition by
                    sm_subscriptions.dim_subscription_id,
                    usage_ping.uuid,
                    usage_ping.hostname,
                    is_data_in_subscription_month
                order by sm_subscriptions.snapshot_month desc
            )
            = 1,
            true,
            false
        ) as is_latest_data
    from sm_subscriptions
    left join
        usage_ping
        on sm_subscriptions.dim_subscription_id = usage_ping.dim_subscription_id
        and sm_subscriptions.snapshot_month = usage_ping.ping_created_at_month
    left join
        seat_link
        on sm_subscriptions.dim_subscription_id = seat_link.dim_subscription_id
        and sm_subscriptions.snapshot_month = seat_link.snapshot_month
    left join
        smau_convert
        on sm_subscriptions.dim_subscription_id = smau_convert.dim_subscription_id
        and sm_subscriptions.snapshot_month = smau_convert.snapshot_month
        and usage_ping.uuid = smau_convert.uuid
        and usage_ping.hostname = smau_convert.hostname

)

{{
    dbt_audit(
        cte_ref="joined",
        created_by="@ischweickartDD",
        updated_by="@mdrussell",
        created_date="2021-02-08",
        updated_date="2021-04-12",
    )
}}

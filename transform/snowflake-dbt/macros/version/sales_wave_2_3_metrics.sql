{% macro sales_wave_2_3_metrics() %}

-- usage ping data - devops metrics (wave 2 & 3.0)
{{
    null_negative_numbers(
        "raw_usage_data_payload['usage_activity_by_stage_monthly']['manage']['events']"
    )
}} as umau_28_days_user,
{{
    null_negative_numbers(
        "raw_usage_data_payload['usage_activity_by_stage_monthly']['create']['action_monthly_active_users_project_repo']"
    )
}}
as action_monthly_active_users_project_repo_28_days_user,
{{
    null_negative_numbers(
        "raw_usage_data_payload['usage_activity_by_stage_monthly']['create']['merge_requests']"
    )
}}
as merge_requests_28_days_user,
{{
    null_negative_numbers(
        "raw_usage_data_payload['usage_activity_by_stage_monthly']['create']['projects_with_repositories_enabled']"
    )
}}
as projects_with_repositories_enabled_28_days_user,
{{ null_negative_numbers("raw_usage_data_payload['counts']['commit_comment']") }}
as commit_comment_all_time_event,
{{ null_negative_numbers("raw_usage_data_payload['counts']['source_code_pushes']") }}
as source_code_pushes_all_time_event,
{{
    null_negative_numbers(
        "raw_usage_data_payload['usage_activity_by_stage_monthly']['verify']['ci_pipelines']"
    )
}}
as ci_pipelines_28_days_user,
{{
    null_negative_numbers(
        "raw_usage_data_payload['usage_activity_by_stage_monthly']['verify']['ci_internal_pipelines']"
    )
}}
as ci_internal_pipelines_28_days_user,
{{
    null_negative_numbers(
        "raw_usage_data_payload['usage_activity_by_stage_monthly']['verify']['ci_builds']"
    )
}}
as ci_builds_28_days_user,
{{
    null_negative_numbers(
        "raw_usage_data_payload['usage_activity_by_stage']['verify']['ci_builds']"
    )
}} as ci_builds_all_time_user,
{{ null_negative_numbers("raw_usage_data_payload['counts']['ci_builds']") }}
as ci_builds_all_time_event,
{{ null_negative_numbers("raw_usage_data_payload['counts']['ci_runners']") }}
as ci_runners_all_time_event,
{{ null_negative_numbers("raw_usage_data_payload['counts']['auto_devops_enabled']") }}
as auto_devops_enabled_all_time_event,
{{
    convert_variant_to_boolean_field(
        "raw_usage_data_payload['gitlab_shared_runners_enabled']"
    )
}} as gitlab_shared_runners_enabled,
{{
    convert_variant_to_boolean_field(
        "raw_usage_data_payload['container_registry_enabled']"
    )
}} as container_registry_enabled,
{{ null_negative_numbers("raw_usage_data_payload['counts']['template_repositories']") }}
as template_repositories_all_time_event,
{{
    null_negative_numbers(
        "raw_usage_data_payload['usage_activity_by_stage_monthly']['verify']['ci_pipeline_config_repository']"
    )
}}
as ci_pipeline_config_repository_28_days_user,
{{
    null_negative_numbers(
        "raw_usage_data_payload['usage_activity_by_stage_monthly']['secure']['user_unique_users_all_secure_scanners']"
    )
}}
as user_unique_users_all_secure_scanners_28_days_user,
{{
    null_negative_numbers(
        "raw_usage_data_payload['usage_activity_by_stage_monthly']['secure']['user_sast_jobs']"
    )
}}
as user_sast_jobs_28_days_user,
{{
    null_negative_numbers(
        "raw_usage_data_payload['usage_activity_by_stage_monthly']['secure']['user_dast_jobs']"
    )
}}
as user_dast_jobs_28_days_user,
{{
    null_negative_numbers(
        "raw_usage_data_payload['usage_activity_by_stage_monthly']['secure']['user_dependency_scanning_jobs']"
    )
}}
as user_dependency_scanning_jobs_28_days_user,
{{
    null_negative_numbers(
        "raw_usage_data_payload['usage_activity_by_stage_monthly']['secure']['user_license_management_jobs']"
    )
}}
as user_license_management_jobs_28_days_user,
{{
    null_negative_numbers(
        "raw_usage_data_payload['usage_activity_by_stage_monthly']['secure']['user_secret_detection_jobs']"
    )
}}
as user_secret_detection_jobs_28_days_user,
{{
    null_negative_numbers(
        "raw_usage_data_payload['usage_activity_by_stage_monthly']['secure']['user_container_scanning_jobs']"
    )
}}
as user_container_scanning_jobs_28_days_user,
{{
    convert_variant_to_boolean_field(
        "raw_usage_data_payload['object_store']['packages']['enabled']"
    )
}} as object_store_packages_enabled,
{{ null_negative_numbers("raw_usage_data_payload['counts']['projects_with_packages']") }}
as projects_with_packages_all_time_event,
{{
    null_negative_numbers(
        "raw_usage_data_payload['usage_activity_by_stage_monthly']['package']['projects_with_packages']"
    )
}}
as projects_with_packages_28_days_user,
{{
    null_negative_numbers(
        "raw_usage_data_payload['usage_activity_by_stage_monthly']['release']['deployments']"
    )
}}
as deployments_28_days_user,
{{
    null_negative_numbers(
        "raw_usage_data_payload['usage_activity_by_stage_monthly']['release']['releases']"
    )
}}
as releases_28_days_user,
{{
    null_negative_numbers(
        "raw_usage_data_payload['usage_activity_by_stage_monthly']['plan']['epics']"
    )
}} as epics_28_days_user,
{{
    null_negative_numbers(
        "raw_usage_data_payload['usage_activity_by_stage_monthly']['plan']['issues']"
    )
}} as issues_28_days_user,

-- 3.1 metrics
{{ null_negative_numbers("raw_usage_data_payload['counts']['ci_internal_pipelines']") }}
as ci_internal_pipelines_all_time_event,
{{ null_negative_numbers("raw_usage_data_payload['counts']['ci_external_pipelines']") }}
as ci_external_pipelines_all_time_event,
{{ null_negative_numbers("raw_usage_data_payload['counts']['merge_requests']") }}
as merge_requests_all_time_event,
{{ null_negative_numbers("raw_usage_data_payload['counts']['todos']") }}
as todos_all_time_event,
{{ null_negative_numbers("raw_usage_data_payload['counts']['epics']") }}
as epics_all_time_event,
{{ null_negative_numbers("raw_usage_data_payload['counts']['issues']") }}
as issues_all_time_event,
{{ null_negative_numbers("raw_usage_data_payload['counts']['projects']") }}
as projects_all_time_event,
{{ null_negative_numbers("raw_usage_data_payload['counts_monthly']['deployments']") }}
as deployments_28_days_event,
{{ null_negative_numbers("raw_usage_data_payload['counts_monthly']['packages']") }}
as packages_28_days_event,
{{ null_negative_numbers("raw_usage_data_payload['counts']['sast_jobs']") }}
as sast_jobs_all_time_event,
{{ null_negative_numbers("raw_usage_data_payload['counts']['dast_jobs']") }}
as dast_jobs_all_time_event,
{{
    null_negative_numbers(
        "raw_usage_data_payload['counts']['dependency_scanning_jobs']"
    )
}} as dependency_scanning_jobs_all_time_event,
{{
    null_negative_numbers(
        "raw_usage_data_payload['counts']['license_management_jobs']"
    )
}} as license_management_jobs_all_time_event,
{{ null_negative_numbers("raw_usage_data_payload['counts']['secret_detection_jobs']") }}
as secret_detection_jobs_all_time_event,
{{
    null_negative_numbers(
        "raw_usage_data_payload['counts']['container_scanning_jobs']"
    )
}} as container_scanning_jobs_all_time_event,
{{
    null_negative_numbers(
        "raw_usage_data_payload['counts']['projects_jenkins_active']"
    )
}} as projects_jenkins_active_all_time_event,
{{ null_negative_numbers("raw_usage_data_payload['counts']['projects_bamboo_active']") }}
as projects_bamboo_active_all_time_event,
{{ null_negative_numbers("raw_usage_data_payload['counts']['projects_jira_active']") }}
as projects_jira_active_all_time_event,
{{
    null_negative_numbers(
        "raw_usage_data_payload['counts']['projects_drone_ci_active']"
    )
}} as projects_drone_ci_active_all_time_event,
-- this metrics is deprecated, keeping it around for historical reference
{{
    null_negative_numbers(
        "raw_usage_data_payload['usage_activity_by_stage_monthly']['manage']['issue_imports']['jira']"
    )
}}
as jira_imports_28_days_event,
{{ null_negative_numbers("raw_usage_data_payload['counts']['projects_github_active']") }}
as projects_github_active_all_time_event,
{{
    null_negative_numbers(
        "raw_usage_data_payload['counts']['projects_jira_server_active']"
    )
}} as projects_jira_server_active_all_time_event,
{{
    null_negative_numbers(
        "raw_usage_data_payload['counts']['projects_jira_dvcs_cloud_active']"
    )
}} as projects_jira_dvcs_cloud_active_all_time_event,
{{
    null_negative_numbers(
        "raw_usage_data_payload['counts']['projects_with_repositories_enabled']"
    )
}} as projects_with_repositories_enabled_all_time_event,
{{ null_negative_numbers("raw_usage_data_payload['counts']['protected_branches']") }}
as protected_branches_all_time_event,
{{ null_negative_numbers("raw_usage_data_payload['counts']['remote_mirrors']") }}
as remote_mirrors_all_time_event,
{{
    null_negative_numbers(
        "raw_usage_data_payload['usage_activity_by_stage_monthly']['create']['projects_enforcing_code_owner_approval']"
    )
}}
as projects_enforcing_code_owner_approval_28_days_user,
{{
    null_negative_numbers(
        "raw_usage_data_payload['usage_activity_by_stage_monthly']['configure']['project_clusters_enabled']"
    )
}}
as project_clusters_enabled_28_days_user,

-- 3.2 metrics
{{
    convert_variant_to_boolean_field(
        "raw_usage_data_payload['instance_auto_devops_enabled']"
    )
}} as auto_devops_enabled,
{{ null_negative_numbers("raw_usage_data_payload['gitaly']['clusters']") }}
as gitaly_clusters_instance,
{{
    null_negative_numbers(
        "raw_usage_data_payload['counts']['epics_deepest_relationship_level']"
    )
}} as epics_deepest_relationship_level_instance,
{{
    null_negative_numbers(
        "raw_usage_data_payload['counts']['clusters_applications_cilium']"
    )
}} as clusters_applications_cilium_all_time_event,
{{
    null_negative_numbers(
        "raw_usage_data_payload['counts']['network_policy_forwards']"
    )
}} as network_policy_forwards_all_time_event,
{{ null_negative_numbers("raw_usage_data_payload['counts']['network_policy_drops']") }}
as network_policy_drops_all_time_event,
{{
    null_negative_numbers(
        "raw_usage_data_payload['counts']['requirements_with_test_report']"
    )
}} as requirements_with_test_report_all_time_event,
{{
    null_negative_numbers(
        "raw_usage_data_payload['counts']['requirement_test_reports_ci']"
    )
}} as requirement_test_reports_ci_all_time_event,
{{
    null_negative_numbers(
        "raw_usage_data_payload['counts']['projects_imported_from_github']"
    )
}} as projects_imported_from_github_all_time_event,
{{
    null_negative_numbers(
        "raw_usage_data_payload['counts']['projects_jira_cloud_active']"
    )
}} as projects_jira_cloud_active_all_time_event,
{{
    null_negative_numbers(
        "raw_usage_data_payload['counts']['projects_jira_dvcs_server_active']"
    )
}} as projects_jira_dvcs_server_active_all_time_event,
{{ null_negative_numbers("raw_usage_data_payload['counts']['service_desk_issues']") }}
as service_desk_issues_all_time_event,
{{
    null_negative_numbers(
        "raw_usage_data_payload['usage_activity_by_stage']['verify']['ci_pipelines']"
    )
}} as ci_pipelines_all_time_user,
{{
    null_negative_numbers(
        "raw_usage_data_payload['usage_activity_by_stage_monthly']['plan']['service_desk_issues']"
    )
}}
as service_desk_issues_28_days_user,
{{
    null_negative_numbers(
        "raw_usage_data_payload['usage_activity_by_stage_monthly']['plan']['projects_jira_active']"
    )
}}
as projects_jira_active_28_days_user,
{{
    null_negative_numbers(
        "raw_usage_data_payload['usage_activity_by_stage_monthly']['plan']['projects_jira_dvcs_cloud_active']"
    )
}}
as projects_jira_dvcs_cloud_active_28_days_user,
{{
    null_negative_numbers(
        "raw_usage_data_payload['usage_activity_by_stage_monthly']['plan']['projects_jira_dvcs_server_active']"
    )
}}
as projects_jira_dvcs_server_active_28_days_user,
{{
    null_negative_numbers(
        "raw_usage_data_payload['usage_activity_by_stage_monthly']['create']['merge_requests_with_required_codeowners']"
    )
}}
as merge_requests_with_required_code_owners_28_days_user,
{{
    null_negative_numbers(
        "raw_usage_data_payload['redis_hll_counters']['analytics']['g_analytics_valuestream_monthly']"
    )
}}
as analytics_value_stream_28_days_event,
{{
    null_negative_numbers(
        "raw_usage_data_payload['redis_hll_counters']['code_review']['i_code_review_user_approve_mr_monthly']"
    )
}}
as code_review_user_approve_mr_28_days_user,
{{
    null_negative_numbers(
        "raw_usage_data_payload['redis_hll_counters']['epics_usage']['epics_usage_total_unique_counts_monthly']"
    )
}}
as epics_usage_28_days_user,
{{
    null_negative_numbers(
        "raw_usage_data_payload['redis_hll_counters']['ci_templates']['ci_templates_total_unique_counts_monthly']"
    )
}}
as ci_templates_usage_28_days_event,
{{
    null_negative_numbers(
        "raw_usage_data_payload['redis_hll_counters']['issues_edit']['g_project_management_issue_milestone_changed_monthly']"
    )
}}
as project_management_issue_milestone_changed_28_days_user,
{{
    null_negative_numbers(
        "raw_usage_data_payload['redis_hll_counters']['issues_edit']['g_project_management_issue_iteration_changed_monthly']"
    )
}}
as project_management_issue_iteration_changed_28_days_user,

-- 5.1 metrics
{{
    null_negative_numbers(
        "raw_usage_data_payload['usage_activity_by_stage_monthly']['create']['protected_branches']"
    )
}}
as protected_branches_28_days_user,
{{
    null_negative_numbers(
        "raw_usage_data_payload['redis_hll_counters']['analytics']['p_analytics_ci_cd_lead_time_monthly']"
    )
}}
as ci_cd_lead_time_usage_28_days_event,
{{
    null_negative_numbers(
        "raw_usage_data_payload['redis_hll_counters']['analytics']['p_analytics_ci_cd_deployment_frequency_monthly']"
    )
}}
as ci_cd_deployment_frequency_usage_28_days_event,
{{
    null_negative_numbers(
        "raw_usage_data_payload['usage_activity_by_stage']['create']['projects_with_repositories_enabled']"
    )
}}
as projects_with_repositories_enabled_all_time_user,
{{
    null_negative_numbers(
        "raw_usage_data_payload['usage_activity_by_stage_monthly']['secure']['user_api_fuzzing_jobs']"
    )
}}
as api_fuzzing_jobs_usage_28_days_user,
{{
    null_negative_numbers(
        "raw_usage_data_payload['usage_activity_by_stage_monthly']['secure']['coverage_fuzzing_pipeline']"
    )
}}
as coverage_fuzzing_pipeline_usage_28_days_event,
{{
    null_negative_numbers(
        "raw_usage_data_payload['usage_activity_by_stage_monthly']['secure']['api_fuzzing_pipeline']"
    )
}}
as api_fuzzing_pipeline_usage_28_days_event,
{{
    null_negative_numbers(
        "raw_usage_data_payload['usage_activity_by_stage_monthly']['secure']['container_scanning_pipeline']"
    )
}}
as container_scanning_pipeline_usage_28_days_event,
{{
    null_negative_numbers(
        "raw_usage_data_payload['usage_activity_by_stage_monthly']['secure']['dependency_scanning_pipeline']"
    )
}}
as dependency_scanning_pipeline_usage_28_days_event,
{{
    null_negative_numbers(
        "raw_usage_data_payload['usage_activity_by_stage_monthly']['secure']['sast_pipeline']"
    )
}}
as sast_pipeline_usage_28_days_event,
{{
    null_negative_numbers(
        "raw_usage_data_payload['usage_activity_by_stage_monthly']['secure']['secret_detection_pipeline']"
    )
}}
as secret_detection_pipeline_usage_28_days_event,
{{
    null_negative_numbers(
        "raw_usage_data_payload['usage_activity_by_stage_monthly']['secure']['dast_pipeline']"
    )
}}
as dast_pipeline_usage_28_days_event,
{{
    null_negative_numbers(
        "raw_usage_data_payload['usage_activity_by_stage_monthly']['secure']['user_coverage_fuzzing_jobs']"
    )
}}
as coverage_fuzzing_jobs_28_days_user,
{{ null_negative_numbers("raw_usage_data_payload['counts']['environments']") }}
as environments_all_time_event,
{{ null_negative_numbers("raw_usage_data_payload['counts']['feature_flags']") }}
as feature_flags_all_time_event,
{{
    null_negative_numbers(
        "raw_usage_data_payload['counts_monthly']['successful_deployments']"
    )
}} as successful_deployments_28_days_event,
{{
    null_negative_numbers(
        "raw_usage_data_payload['counts_monthly']['failed_deployments']"
    )
}} as failed_deployments_28_days_event,
{{
    null_negative_numbers(
        "raw_usage_data_payload['usage_activity_by_stage_monthly']['manage']['projects_with_compliance_framework']"
    )
}}
as projects_compliance_framework_all_time_event,
{{
    null_negative_numbers(
        "raw_usage_data_payload['redis_hll_counters']['pipeline_authoring']['o_pipeline_authoring_unique_users_committing_ciconfigfile_monthly']"
    )
}}
as commit_ci_config_file_28_days_user,
{{
    null_negative_numbers(
        "raw_usage_data_payload['compliance_unique_visits']['g_compliance_audit_events']"
    )
}}
as view_audit_all_time_user,

-- 5.2 metrics
{{
    null_negative_numbers(
        "raw_usage_data_payload['usage_activity_by_stage']['secure']['user_dependency_scanning_jobs']"
    )
}}
as dependency_scanning_jobs_all_time_user,
{{
    null_negative_numbers(
        "raw_usage_data_payload['analytics_unique_visits']['i_analytics_dev_ops_adoption']"
    )
}}
as analytics_devops_adoption_all_time_user,
{{
    null_negative_numbers(
        "raw_usage_data_payload['usage_activity_by_stage']['manage']['project_imports']['total']"
    )
}}
as projects_imported_all_time_event,
{{
    null_negative_numbers(
        "raw_usage_data_payload['usage_activity_by_stage_monthly']['secure']['user_preferences_group_overview_security_dashboard']"
    )
}}
as preferences_security_dashboard_28_days_user,
{{
    null_negative_numbers(
        "raw_usage_data_payload['usage_activity_by_stage_monthly']['create']['action_monthly_active_users_ide_edit']"
    )
}}
as web_ide_edit_28_days_user,
{{
    null_negative_numbers(
        "raw_usage_data_payload['counts']['ci_pipeline_config_auto_devops']"
    )
}} as auto_devops_pipelines_all_time_event,
{{
    null_negative_numbers(
        "raw_usage_data_payload['counts']['projects_prometheus_active']"
    )
}} as projects_prometheus_active_all_time_event,
{{ convert_variant_to_boolean_field("raw_usage_data_payload['prometheus_enabled']") }}
as prometheus_enabled,
{{
    convert_variant_to_boolean_field(
        "raw_usage_data_payload['prometheus_metrics_enabled']"
    )
}} as prometheus_metrics_enabled,
{{
    convert_variant_to_boolean_field(
        "raw_usage_data_payload['usage_activity_by_stage']['manage']['group_saml_enabled']"
    )
}}
as group_saml_enabled,
{{
    null_negative_numbers(
        "raw_usage_data_payload['usage_activity_by_stage']['manage']['issue_imports']['jira']"
    )
}}
as jira_issue_imports_all_time_event,
{{
    null_negative_numbers(
        "raw_usage_data_payload['usage_activity_by_stage']['plan']['epics']"
    )
}} as author_epic_all_time_user,
{{
    null_negative_numbers(
        "raw_usage_data_payload['usage_activity_by_stage']['plan']['issues']"
    )
}} as author_issue_all_time_user,
{{
    null_negative_numbers(
        "raw_usage_data_payload['usage_activity_by_stage_monthly']['release']['failed_deployments']"
    )
}}
as failed_deployments_28_days_user,
{{
    null_negative_numbers(
        "raw_usage_data_payload['usage_activity_by_stage_monthly']['release']['successful_deployments']"
    )
}}
as successful_deployments_28_days_user,

-- 5.3 metrics
{{ convert_variant_to_boolean_field("raw_usage_data_payload['geo_enabled']") }}
as geo_enabled,
{{ null_negative_numbers("raw_usage_data_payload['counts']['geo_nodes']") }}
as geo_nodes_all_time_event,
{{
    null_negative_numbers(
        "raw_usage_data_payload['usage_activity_by_stage_monthly']['verify']['ci_pipeline_config_auto_devops']"
    )
}}
as auto_devops_pipelines_28_days_user,
{{
    null_negative_numbers(
        "raw_usage_data_payload['counts']['ci_runners_instance_type_active']"
    )
}} as active_instance_runners_all_time_event,
{{
    null_negative_numbers(
        "raw_usage_data_payload['counts']['ci_runners_group_type_active']"
    )
}} as active_group_runners_all_time_event,
{{
    null_negative_numbers(
        "raw_usage_data_payload['counts']['ci_runners_project_type_active']"
    )
}} as active_project_runners_all_time_event,
raw_usage_data_payload['gitaly'] ['version']::varchar as gitaly_version,
{{ null_negative_numbers("raw_usage_data_payload['gitaly']['servers']") }}
as gitaly_servers_all_time_event

{%- endmacro -%}

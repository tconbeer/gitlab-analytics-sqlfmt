{% macro sales_wave_2_3_metrics() %}

    -- usage ping data - devops metrics (wave 2 & 3.0)
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['manage']['events']") }}                                        AS umau_28_days_user,
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['create']['action_monthly_active_users_project_repo']") }}      AS action_monthly_active_users_project_repo_28_days_user,
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['create']['merge_requests']") }}                                AS merge_requests_28_days_user,
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['create']['projects_with_repositories_enabled']") }}            AS projects_with_repositories_enabled_28_days_user,
    {{ null_negative_numbers("raw_usage_data_payload['counts']['commit_comment']") }}                                                                   AS commit_comment_all_time_event,
    {{ null_negative_numbers("raw_usage_data_payload['counts']['source_code_pushes']") }}                                                               AS source_code_pushes_all_time_event,
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['verify']['ci_pipelines']") }}                                  AS ci_pipelines_28_days_user,
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['verify']['ci_internal_pipelines']") }}                         AS ci_internal_pipelines_28_days_user,
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['verify']['ci_builds']") }}                                     AS ci_builds_28_days_user,
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage']['verify']['ci_builds']") }}                                             AS ci_builds_all_time_user,
    {{ null_negative_numbers("raw_usage_data_payload['counts']['ci_builds']") }}                                                                        AS ci_builds_all_time_event,
    {{ null_negative_numbers("raw_usage_data_payload['counts']['ci_runners']") }}                                                                       AS ci_runners_all_time_event,
    {{ null_negative_numbers("raw_usage_data_payload['counts']['auto_devops_enabled']") }}                                                              AS auto_devops_enabled_all_time_event,
    {{ convert_variant_to_boolean_field("raw_usage_data_payload['gitlab_shared_runners_enabled']") }}                                                   AS gitlab_shared_runners_enabled,
    {{ convert_variant_to_boolean_field("raw_usage_data_payload['container_registry_enabled']") }}                                                      AS container_registry_enabled,
    {{ null_negative_numbers("raw_usage_data_payload['counts']['template_repositories']") }}                                                            AS template_repositories_all_time_event,
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['verify']['ci_pipeline_config_repository']") }}                 AS ci_pipeline_config_repository_28_days_user,
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['secure']['user_unique_users_all_secure_scanners']") }}         AS user_unique_users_all_secure_scanners_28_days_user,
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['secure']['user_sast_jobs']") }}                                AS user_sast_jobs_28_days_user,
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['secure']['user_dast_jobs']") }}                                AS user_dast_jobs_28_days_user,
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['secure']['user_dependency_scanning_jobs']") }}                 AS user_dependency_scanning_jobs_28_days_user,
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['secure']['user_license_management_jobs']") }}                  AS user_license_management_jobs_28_days_user,
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['secure']['user_secret_detection_jobs']") }}                    AS user_secret_detection_jobs_28_days_user,
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['secure']['user_container_scanning_jobs']") }}                  AS user_container_scanning_jobs_28_days_user,
    {{ convert_variant_to_boolean_field("raw_usage_data_payload['object_store']['packages']['enabled']") }}                                             AS object_store_packages_enabled,
    {{ null_negative_numbers("raw_usage_data_payload['counts']['projects_with_packages']") }}                                                           AS projects_with_packages_all_time_event,
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['package']['projects_with_packages']") }}                       AS projects_with_packages_28_days_user,
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['release']['deployments']") }}                                  AS deployments_28_days_user,
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['release']['releases']") }}                                     AS releases_28_days_user,
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['plan']['epics']") }}                                           AS epics_28_days_user,
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['plan']['issues']") }}                                          AS issues_28_days_user,

    -- 3.1 metrics

    {{ null_negative_numbers("raw_usage_data_payload['counts']['ci_internal_pipelines']") }}                                                            AS ci_internal_pipelines_all_time_event,
    {{ null_negative_numbers("raw_usage_data_payload['counts']['ci_external_pipelines']") }}                                                            AS ci_external_pipelines_all_time_event,
    {{ null_negative_numbers("raw_usage_data_payload['counts']['merge_requests']") }}                                                                   AS merge_requests_all_time_event,
    {{ null_negative_numbers("raw_usage_data_payload['counts']['todos']") }}                                                                            AS todos_all_time_event,
    {{ null_negative_numbers("raw_usage_data_payload['counts']['epics']") }}                                                                            AS epics_all_time_event,
    {{ null_negative_numbers("raw_usage_data_payload['counts']['issues']") }}                                                                           AS issues_all_time_event,
    {{ null_negative_numbers("raw_usage_data_payload['counts']['projects']") }}                                                                         AS projects_all_time_event,
    {{ null_negative_numbers("raw_usage_data_payload['counts_monthly']['deployments']") }}                                                              AS deployments_28_days_event,
    {{ null_negative_numbers("raw_usage_data_payload['counts_monthly']['packages']") }}                                                                 AS packages_28_days_event,
    {{ null_negative_numbers("raw_usage_data_payload['counts']['sast_jobs']") }}                                                                        AS sast_jobs_all_time_event,
    {{ null_negative_numbers("raw_usage_data_payload['counts']['dast_jobs']") }}                                                                        AS dast_jobs_all_time_event,
    {{ null_negative_numbers("raw_usage_data_payload['counts']['dependency_scanning_jobs']") }}                                                         AS dependency_scanning_jobs_all_time_event,
    {{ null_negative_numbers("raw_usage_data_payload['counts']['license_management_jobs']") }}                                                          AS license_management_jobs_all_time_event,
    {{ null_negative_numbers("raw_usage_data_payload['counts']['secret_detection_jobs']") }}                                                            AS secret_detection_jobs_all_time_event,
    {{ null_negative_numbers("raw_usage_data_payload['counts']['container_scanning_jobs']") }}                                                          AS container_scanning_jobs_all_time_event,
    {{ null_negative_numbers("raw_usage_data_payload['counts']['projects_jenkins_active']") }}                                                          AS projects_jenkins_active_all_time_event,
    {{ null_negative_numbers("raw_usage_data_payload['counts']['projects_bamboo_active']") }}                                                           AS projects_bamboo_active_all_time_event,
    {{ null_negative_numbers("raw_usage_data_payload['counts']['projects_jira_active']") }}                                                             AS projects_jira_active_all_time_event,
    {{ null_negative_numbers("raw_usage_data_payload['counts']['projects_drone_ci_active']") }}                                                         AS projects_drone_ci_active_all_time_event,
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['manage']['issue_imports']['jira']") }}                         AS jira_imports_28_days_event, --this metrics is deprecated, keeping it around for historical reference
    {{ null_negative_numbers("raw_usage_data_payload['counts']['projects_github_active']") }}                                                           AS projects_github_active_all_time_event,
    {{ null_negative_numbers("raw_usage_data_payload['counts']['projects_jira_server_active']") }}                                                      AS projects_jira_server_active_all_time_event,
    {{ null_negative_numbers("raw_usage_data_payload['counts']['projects_jira_dvcs_cloud_active']") }}                                                  AS projects_jira_dvcs_cloud_active_all_time_event,
    {{ null_negative_numbers("raw_usage_data_payload['counts']['projects_with_repositories_enabled']") }}                                               AS projects_with_repositories_enabled_all_time_event,
    {{ null_negative_numbers("raw_usage_data_payload['counts']['protected_branches']") }}                                                               AS protected_branches_all_time_event,
    {{ null_negative_numbers("raw_usage_data_payload['counts']['remote_mirrors']") }}                                                                   AS remote_mirrors_all_time_event,
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['create']['projects_enforcing_code_owner_approval']") }}        AS projects_enforcing_code_owner_approval_28_days_user,
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['configure']['project_clusters_enabled']") }}                   AS project_clusters_enabled_28_days_user,

    -- 3.2 metrics

    {{ convert_variant_to_boolean_field("raw_usage_data_payload['instance_auto_devops_enabled']") }}                                                    AS auto_devops_enabled,
    {{ null_negative_numbers("raw_usage_data_payload['gitaly']['clusters']") }}                                                                         AS gitaly_clusters_instance,
    {{ null_negative_numbers("raw_usage_data_payload['counts']['epics_deepest_relationship_level']") }}                                                 AS epics_deepest_relationship_level_instance,
    {{ null_negative_numbers("raw_usage_data_payload['counts']['clusters_applications_cilium']") }}                                                     AS clusters_applications_cilium_all_time_event,
    {{ null_negative_numbers("raw_usage_data_payload['counts']['network_policy_forwards']") }}                                                          AS network_policy_forwards_all_time_event,
    {{ null_negative_numbers("raw_usage_data_payload['counts']['network_policy_drops']") }}                                                             AS network_policy_drops_all_time_event,
    {{ null_negative_numbers("raw_usage_data_payload['counts']['requirements_with_test_report']") }}                                                    AS requirements_with_test_report_all_time_event,
    {{ null_negative_numbers("raw_usage_data_payload['counts']['requirement_test_reports_ci']") }}                                                      AS requirement_test_reports_ci_all_time_event,
    {{ null_negative_numbers("raw_usage_data_payload['counts']['projects_imported_from_github']") }}                                                    AS projects_imported_from_github_all_time_event,
    {{ null_negative_numbers("raw_usage_data_payload['counts']['projects_jira_cloud_active']") }}                                                       AS projects_jira_cloud_active_all_time_event,
    {{ null_negative_numbers("raw_usage_data_payload['counts']['projects_jira_dvcs_server_active']") }}                                                 AS projects_jira_dvcs_server_active_all_time_event,
    {{ null_negative_numbers("raw_usage_data_payload['counts']['service_desk_issues']") }}                                                              AS service_desk_issues_all_time_event,
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage']['verify']['ci_pipelines']") }}                                          AS ci_pipelines_all_time_user,
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['plan']['service_desk_issues']") }}                             AS service_desk_issues_28_days_user,
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['plan']['projects_jira_active']") }}                            AS projects_jira_active_28_days_user,
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['plan']['projects_jira_dvcs_cloud_active']") }}                 AS projects_jira_dvcs_cloud_active_28_days_user,
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['plan']['projects_jira_dvcs_server_active']") }}                AS projects_jira_dvcs_server_active_28_days_user,
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['create']['merge_requests_with_required_codeowners']") }}       AS merge_requests_with_required_code_owners_28_days_user,
    {{ null_negative_numbers("raw_usage_data_payload['redis_hll_counters']['analytics']['g_analytics_valuestream_monthly']") }}                         AS analytics_value_stream_28_days_event,
    {{ null_negative_numbers("raw_usage_data_payload['redis_hll_counters']['code_review']['i_code_review_user_approve_mr_monthly']") }}                 AS code_review_user_approve_mr_28_days_user,
    {{ null_negative_numbers("raw_usage_data_payload['redis_hll_counters']['epics_usage']['epics_usage_total_unique_counts_monthly']") }}               AS epics_usage_28_days_user,
    {{ null_negative_numbers("raw_usage_data_payload['redis_hll_counters']['ci_templates']['ci_templates_total_unique_counts_monthly']") }}             AS ci_templates_usage_28_days_event,
    {{ null_negative_numbers("raw_usage_data_payload['redis_hll_counters']['issues_edit']['g_project_management_issue_milestone_changed_monthly']") }}  AS project_management_issue_milestone_changed_28_days_user,
    {{ null_negative_numbers("raw_usage_data_payload['redis_hll_counters']['issues_edit']['g_project_management_issue_iteration_changed_monthly']") }}  AS project_management_issue_iteration_changed_28_days_user,

    -- 5.1 metrics
    
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['create']['protected_branches']") }}                            AS protected_branches_28_days_user,
    {{ null_negative_numbers("raw_usage_data_payload['redis_hll_counters']['analytics']['p_analytics_ci_cd_lead_time_monthly']") }}                     AS ci_cd_lead_time_usage_28_days_event,
    {{ null_negative_numbers("raw_usage_data_payload['redis_hll_counters']['analytics']['p_analytics_ci_cd_deployment_frequency_monthly']") }}          AS ci_cd_deployment_frequency_usage_28_days_event,
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage']['create']['projects_with_repositories_enabled']") }}                    AS projects_with_repositories_enabled_all_time_user,
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['secure']['user_api_fuzzing_jobs']") }}                         AS api_fuzzing_jobs_usage_28_days_user,
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['secure']['coverage_fuzzing_pipeline']") }}                     AS coverage_fuzzing_pipeline_usage_28_days_event,
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['secure']['api_fuzzing_pipeline']") }}                          AS api_fuzzing_pipeline_usage_28_days_event,
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['secure']['container_scanning_pipeline']") }}                   AS container_scanning_pipeline_usage_28_days_event,
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['secure']['dependency_scanning_pipeline']") }}                  AS dependency_scanning_pipeline_usage_28_days_event,
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['secure']['sast_pipeline']") }}                                 AS sast_pipeline_usage_28_days_event,
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['secure']['secret_detection_pipeline']") }}                     AS secret_detection_pipeline_usage_28_days_event,
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['secure']['dast_pipeline']") }}                                 AS dast_pipeline_usage_28_days_event,
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['secure']['user_coverage_fuzzing_jobs']") }}                    AS coverage_fuzzing_jobs_28_days_user,
    {{ null_negative_numbers("raw_usage_data_payload['counts']['environments']") }}                                                                     AS environments_all_time_event,
    {{ null_negative_numbers("raw_usage_data_payload['counts']['feature_flags']") }}                                                                    AS feature_flags_all_time_event,
    {{ null_negative_numbers("raw_usage_data_payload['counts_monthly']['successful_deployments']") }}                                                   AS successful_deployments_28_days_event,
    {{ null_negative_numbers("raw_usage_data_payload['counts_monthly']['failed_deployments']") }}                                                       AS failed_deployments_28_days_event,
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['manage']['projects_with_compliance_framework']") }}            AS projects_compliance_framework_all_time_event,
    {{ null_negative_numbers("raw_usage_data_payload['redis_hll_counters']['pipeline_authoring']['o_pipeline_authoring_unique_users_committing_ciconfigfile_monthly']") }}  AS commit_ci_config_file_28_days_user,
    {{ null_negative_numbers("raw_usage_data_payload['compliance_unique_visits']['g_compliance_audit_events']") }}                                      AS view_audit_all_time_user,
    
    -- 5.2 metrics
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['secure']['user_dependency_scanning_jobs']") }}                 AS dependency_scanning_jobs_28_days_user,
    {{ null_negative_numbers("raw_usage_data_payload['analytics_unique_visits']['i_analytics_dev_ops_adoption']") }}                                    AS analytics_devops_adoption_all_time_user,
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage']['manage']['project_imports']['total']") }}                              AS projects_imported_all_time_event,
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['secure']['user_preferences_group_overview_security_dashboard']") }}  AS preferences_security_dashboard_28_days_user,
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['create']['action_monthly_active_users_ide_edit']") }}          AS web_ide_edit_28_days_user,
    {{ null_negative_numbers("raw_usage_data_payload['counts']['ci_pipeline_config_auto_devops']") }}                                                   AS auto_devops_pipelines_all_time_event,
    {{ null_negative_numbers("raw_usage_data_payload['counts']['projects_prometheus_active']") }}                                                       AS projects_prometheus_active_all_time_event,
    {{ convert_variant_to_boolean_field("raw_usage_data_payload['prometheus_enabled']") }}                                                              AS prometheus_enabled,
    {{ convert_variant_to_boolean_field("raw_usage_data_payload['prometheus_metrics_enabled']") }}                                                      AS prometheus_metrics_enabled,
    {{ convert_variant_to_boolean_field("raw_usage_data_payload['usage_activity_by_stage']['manage']['group_saml_enabled']") }}                         AS group_saml_enabled,
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage']['manage']['issue_imports']['jira']") }}                                 AS jira_issue_imports_all_time_event,
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage']['plan']['epics']") }}                                                   AS author_epic_all_time_user,
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage']['plan']['issues']") }}                                                  AS author_issue_all_time_user,
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['release']['failed_deployments']") }}                           AS failed_deployments_28_days_user,
    {{ null_negative_numbers("raw_usage_data_payload['usage_activity_by_stage_monthly']['release']['successful_deployments']") }}                       AS successful_deployments_28_days_user

{%- endmacro -%}

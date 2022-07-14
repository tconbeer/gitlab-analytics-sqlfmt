{{
    simple_cte(
        [
            ("marketing_contact", "dim_marketing_contact"),
            ("marketing_contact_role", "bdg_marketing_contact_role"),
            ("namespace_lineage", "prep_namespace"),
            ("project", "prep_project"),
            ("gitlab_namespaces", "gitlab_dotcom_namespaces_source"),
            (
                "usage_ping_subscription_smau",
                "fct_usage_ping_subscription_mapped_smau",
            ),
            ("product_usage_wave_1_3", "fct_product_usage_wave_1_3_metrics_monthly"),
        ]
    )
}},
namespace_project_visibility as (

    select
        dim_namespace_id,
        max(
            iff(visibility_level = 'public', true, false)
        ) as does_namespace_have_public_project
    from project
    group by 1

),
free_namespace_project_visibility as (

    select
        project.dim_namespace_id,
        max(
            iff(
                namespace_lineage.gitlab_plan_title = 'Free'
                and project.visibility_level = 'public',
                true,
                false
            )
        ) as does_free_namespace_have_public_project
    from project
    left join
        namespace_lineage
        on project.dim_namespace_id = namespace_lineage.dim_namespace_id
    group by 1

),
saas_namespace_subscription as (

    select *
    from {{ ref("bdg_namespace_order_subscription") }}
    where is_subscription_active = true or dim_subscription_id is null

),
self_managed_namespace_subscription as (

    select *
    from {{ ref("bdg_self_managed_order_subscription") }}
    where is_subscription_active = true or dim_subscription_id is null

),
usage_ping_subscription_smau_aggregate as (

    select
        dim_subscription_id,
        manage_analytics_total_unique_counts_monthly,
        plan_redis_hll_counters_issues_edit_issues_edit_total_unique_counts_monthly,
        create_repo_writes,
        verify_ci_pipelines_users_28_days,
        package_redis_hll_counters_user_packages_user_packages_total_unique_counts_monthly,
        release_release_creation_users_28_days,
        configure_redis_hll_counters_terraform_p_terraform_state_api_unique_users_monthly,
        monitor_incident_management_activer_user_28_days,
        secure_secure_scanners_users_28_days,
        protect_container_scanning_jobs_users_28_days
    from usage_ping_subscription_smau
    where snapshot_month = date_trunc(month, current_date)

),
product_usage_wave_1_3_aggregate as (

    select
        dim_subscription_id,
        umau_28_days_user,
        action_monthly_active_users_project_repo_28_days_user,
        merge_requests_28_days_user,
        commit_comment_all_time_event,
        source_code_pushes_all_time_event,
        ci_pipelines_28_days_user,
        ci_internal_pipelines_28_days_user,
        ci_builds_28_days_user,
        ci_builds_all_time_user,
        ci_builds_all_time_event,
        ci_runners_all_time_event,
        auto_devops_enabled_all_time_event,
        template_repositories_all_time_event,
        ci_pipeline_config_repository_28_days_user,
        user_unique_users_all_secure_scanners_28_days_user,
        user_container_scanning_jobs_28_days_user,
        user_sast_jobs_28_days_user,
        user_dast_jobs_28_days_user,
        user_dependency_scanning_jobs_28_days_user,
        user_license_management_jobs_28_days_user,
        user_secret_detection_jobs_28_days_user,
        projects_with_packages_all_time_event,
        projects_with_packages_28_days_user,
        deployments_28_days_user,
        releases_28_days_user,
        epics_28_days_user,
        issues_28_days_user,
        instance_user_count_not_aligned,
        historical_max_users_not_aligned
    from product_usage_wave_1_3
    where snapshot_month = date_trunc(month, current_date)

),
prep as (

    select
        marketing_contact.dim_marketing_contact_id,
        marketing_contact_role.marketing_contact_role,
        marketing_contact.email_address,
        coalesce(
            marketing_contact_role.namespace_id,
            saas_namespace.dim_namespace_id,
            saas_customer.dim_namespace_id,
            saas_billing_account.dim_namespace_id
        ) as dim_namespace_id,
        gitlab_namespaces.namespace_path,
        case
            when namespace_lineage.namespace_type = 'User' then 1 else 0
        end as is_individual_namespace,
        case
            when namespace_lineage.namespace_type = 'Group' then 1 else 0
        end as is_group_namespace,
        namespace_lineage.is_setup_for_company as is_setup_for_company,
        namespace_project_visibility.does_namespace_have_public_project
        as does_namespace_have_public_project,
        free_namespace_project_visibility.does_free_namespace_have_public_project
        as does_free_namespace_have_public_project,
        marketing_contact_role.customer_db_customer_id as customer_id,
        marketing_contact_role.zuora_billing_account_id as dim_billing_account_id,
        case
            when saas_namespace.dim_namespace_id is not null
            then saas_namespace.dim_subscription_id
            when saas_customer.dim_namespace_id is not null
            then saas_customer.dim_subscription_id
            when saas_billing_account.dim_namespace_id is not null
            then saas_billing_account.dim_subscription_id
            when self_managed_customer.customer_id is not null
            then self_managed_customer.dim_subscription_id
            when self_managed_billing_account.customer_id is not null
            then self_managed_billing_account.dim_subscription_id
        end as dim_subscription_id,
        case
            when saas_namespace.dim_namespace_id is not null
            then saas_namespace.subscription_start_date
            when saas_customer.dim_namespace_id is not null
            then saas_customer.subscription_start_date
            when saas_billing_account.dim_namespace_id is not null
            then saas_billing_account.subscription_start_date
            when self_managed_customer.customer_id is not null
            then self_managed_customer.subscription_start_date
            when self_managed_billing_account.customer_id is not null
            then self_managed_billing_account.subscription_start_date
        end as subscription_start_date,
        case
            when saas_namespace.dim_namespace_id is not null
            then saas_namespace.subscription_end_date
            when saas_customer.dim_namespace_id is not null
            then saas_customer.subscription_end_date
            when saas_billing_account.dim_namespace_id is not null
            then saas_billing_account.subscription_end_date
            when self_managed_customer.customer_id is not null
            then self_managed_customer.subscription_end_date
            when self_managed_billing_account.customer_id is not null
            then self_managed_billing_account.subscription_end_date
        end as subscription_end_date,
        case
            when
                marketing_contact_role.namespace_id is not null
                and saas_namespace.product_tier_name_namespace is null
            then 'SaaS - Free'
            when
                marketing_contact_role.marketing_contact_role
                in (
                    'Personal Namespace Owner',
                    'Group Namespace Owner',
                    'Group Namespace Member'
                )
            then saas_namespace.product_tier_name_namespace
            when marketing_contact_role.marketing_contact_role in ('Customer DB Owner')
            then saas_customer.product_tier_name_with_trial
            when
                marketing_contact_role.marketing_contact_role
                in ('Zuora Billing Contact')
            then saas_billing_account.product_tier_name_subscription
        end as saas_product_tier,
        case
            when marketing_contact_role.marketing_contact_role in ('Customer DB Owner')
            then self_managed_customer.product_tier_name_with_trial
            when
                marketing_contact_role.marketing_contact_role
                in ('Zuora Billing Contact')
            then self_managed_billing_account.product_tier_name_subscription
        end as self_managed_product_tier,
        case
            when
                saas_namespace.product_tier_name_with_trial = 'SaaS - Trial: Ultimate'
                or saas_customer.order_is_trial = true
            then 1
            else 0
        end as is_saas_trial,
        current_date
        - cast(
            saas_namespace.saas_trial_expired_on as date
        ) as days_since_saas_trial_ended,
        {{ days_buckets("days_since_saas_trial_ended") }}
        as days_since_saas_trial_ended_bucket,
        case
            when saas_customer.order_is_trial
            then cast(saas_customer.order_end_date as date)
            when saas_namespace.product_tier_name_with_trial = 'SaaS - Trial: Ultimate'
            then
                cast(
                    coalesce(
                        saas_namespace.saas_trial_expired_on,
                        saas_namespace.order_end_date
                    ) as date
                )
        end as trial_end_date,
        case
            when trial_end_date is not null and current_date <= trial_end_date
            then trial_end_date - current_date
        end as days_until_saas_trial_ends,
        {{ days_buckets("days_until_saas_trial_ends") }}
        as days_until_saas_trial_ends_bucket,
        case
            when saas_product_tier = 'SaaS - Free' then 1 else 0
        end as is_saas_free_tier,
        case
            when saas_product_tier = 'SaaS - Bronze' then 1 else 0
        end as is_saas_bronze_tier,
        case
            when saas_product_tier = 'SaaS - Premium' then 1 else 0
        end as is_saas_premium_tier,
        case
            when saas_product_tier = 'SaaS - Ultimate' then 1 else 0
        end as is_saas_ultimate_tier,
        case
            when self_managed_product_tier = 'Self-Managed - Starter' then 1 else 0
        end as is_self_managed_starter_tier,
        case
            when self_managed_product_tier = 'Self-Managed - Premium' then 1 else 0
        end as is_self_managed_premium_tier,
        case
            when self_managed_product_tier = 'Self-Managed - Ultimate'
            then 1
            else 0
        end as is_self_managed_ultimate_tier

    from marketing_contact_role
    inner join
        marketing_contact
        on marketing_contact.dim_marketing_contact_id
        = marketing_contact_role.dim_marketing_contact_id
    left join
        saas_namespace_subscription saas_namespace
        on saas_namespace.dim_namespace_id = marketing_contact_role.namespace_id
    left join
        saas_namespace_subscription saas_customer
        on saas_customer.customer_id = marketing_contact_role.customer_db_customer_id
    left join
        saas_namespace_subscription saas_billing_account
        on saas_billing_account.dim_billing_account_id
        = marketing_contact_role.zuora_billing_account_id
    left join
        self_managed_namespace_subscription self_managed_customer
        on self_managed_customer.customer_id
        = marketing_contact_role.customer_db_customer_id
    left join
        self_managed_namespace_subscription self_managed_billing_account
        on self_managed_billing_account.dim_billing_account_id
        = marketing_contact_role.zuora_billing_account_id
    left join
        namespace_lineage
        on namespace_lineage.dim_namespace_id
        = coalesce(
            marketing_contact_role.namespace_id,
            saas_namespace.dim_namespace_id,
            saas_customer.dim_namespace_id,
            saas_billing_account.dim_namespace_id
        )
    left join
        gitlab_namespaces
        on namespace_lineage.dim_namespace_id = gitlab_namespaces.namespace_id
    left join
        namespace_project_visibility
        on namespace_lineage.dim_namespace_id
        = namespace_project_visibility.dim_namespace_id
    left join
        free_namespace_project_visibility
        on namespace_lineage.dim_namespace_id
        = free_namespace_project_visibility.dim_namespace_id

),
final as (

    select
        prep.*,
        usage_ping_subscription_smau_aggregate.manage_analytics_total_unique_counts_monthly
        as smau_manage_analytics_total_unique_counts_monthly,
        usage_ping_subscription_smau_aggregate.plan_redis_hll_counters_issues_edit_issues_edit_total_unique_counts_monthly
        as smau_plan_redis_hll_counters_issues_edit_issues_edit_total_unique_counts_monthly,
        usage_ping_subscription_smau_aggregate.create_repo_writes
        as smau_create_repo_writes,
        usage_ping_subscription_smau_aggregate.verify_ci_pipelines_users_28_days
        as smau_verify_ci_pipelines_users_28_days,
        usage_ping_subscription_smau_aggregate.package_redis_hll_counters_user_packages_user_packages_total_unique_counts_monthly
        as smau_package_redis_hll_counters_user_packages_user_packages_total_unique_counts_monthly,
        usage_ping_subscription_smau_aggregate.release_release_creation_users_28_days
        as smau_release_release_creation_users_28_days,
        usage_ping_subscription_smau_aggregate.configure_redis_hll_counters_terraform_p_terraform_state_api_unique_users_monthly
        as smau_configure_redis_hll_counters_terraform_p_terraform_state_api_unique_users_monthly,
        usage_ping_subscription_smau_aggregate.monitor_incident_management_activer_user_28_days
        as smau_monitor_incident_management_activer_user_28_days,
        usage_ping_subscription_smau_aggregate.secure_secure_scanners_users_28_days
        as smau_secure_secure_scanners_users_28_days,
        usage_ping_subscription_smau_aggregate.protect_container_scanning_jobs_users_28_days
        as smau_protect_container_scanning_jobs_users_28_days,
        product_usage_wave_1_3_aggregate.umau_28_days_user as usage_umau_28_days_user,
        product_usage_wave_1_3_aggregate.action_monthly_active_users_project_repo_28_days_user
        as usage_action_monthly_active_users_project_repo_28_days_user,
        product_usage_wave_1_3_aggregate.merge_requests_28_days_user
        as usage_merge_requests_28_days_user,
        product_usage_wave_1_3_aggregate.commit_comment_all_time_event
        as usage_commit_comment_all_time_event,
        product_usage_wave_1_3_aggregate.source_code_pushes_all_time_event
        as usage_source_code_pushes_all_time_event,
        product_usage_wave_1_3_aggregate.ci_pipelines_28_days_user
        as usage_ci_pipelines_28_days_user,
        product_usage_wave_1_3_aggregate.ci_internal_pipelines_28_days_user
        as usage_ci_internal_pipelines_28_days_user,
        product_usage_wave_1_3_aggregate.ci_builds_28_days_user
        as usage_ci_builds_28_days_user,
        product_usage_wave_1_3_aggregate.ci_builds_all_time_user
        as usage_ci_builds_all_time_user,
        product_usage_wave_1_3_aggregate.ci_builds_all_time_event
        as usage_ci_builds_all_time_event,
        product_usage_wave_1_3_aggregate.ci_runners_all_time_event
        as usage_ci_runners_all_time_event,
        product_usage_wave_1_3_aggregate.auto_devops_enabled_all_time_event
        as usage_auto_devops_enabled_all_time_event,
        product_usage_wave_1_3_aggregate.template_repositories_all_time_event
        as usage_template_repositories_all_time_event,
        product_usage_wave_1_3_aggregate.ci_pipeline_config_repository_28_days_user
        as usage_ci_pipeline_config_repository_28_days_user,
        product_usage_wave_1_3_aggregate.user_unique_users_all_secure_scanners_28_days_user
        as usage_user_unique_users_all_secure_scanners_28_days_user,
        product_usage_wave_1_3_aggregate.user_container_scanning_jobs_28_days_user
        as usage_user_container_scanning_jobs_28_days_user,
        product_usage_wave_1_3_aggregate.user_sast_jobs_28_days_user
        as usage_user_sast_jobs_28_days_user,
        product_usage_wave_1_3_aggregate.user_dast_jobs_28_days_user
        as usage_user_dast_jobs_28_days_user,
        product_usage_wave_1_3_aggregate.user_dependency_scanning_jobs_28_days_user
        as usage_user_dependency_scanning_jobs_28_days_user,
        product_usage_wave_1_3_aggregate.user_license_management_jobs_28_days_user
        as usage_user_license_management_jobs_28_days_user,
        product_usage_wave_1_3_aggregate.user_secret_detection_jobs_28_days_user
        as usage_user_secret_detection_jobs_28_days_user,
        product_usage_wave_1_3_aggregate.projects_with_packages_all_time_event
        as usage_projects_with_packages_all_time_event,
        product_usage_wave_1_3_aggregate.projects_with_packages_28_days_user
        as usage_projects_with_packages_28_days_user,
        product_usage_wave_1_3_aggregate.deployments_28_days_user
        as usage_deployments_28_days_user,
        product_usage_wave_1_3_aggregate.releases_28_days_user
        as usage_releases_28_days_user,
        product_usage_wave_1_3_aggregate.epics_28_days_user as usage_epics_28_days_user,
        product_usage_wave_1_3_aggregate.issues_28_days_user
        as usage_issues_28_days_user,
        product_usage_wave_1_3_aggregate.instance_user_count_not_aligned
        as usage_instance_user_count_not_aligned,
        product_usage_wave_1_3_aggregate.historical_max_users_not_aligned
        as usage_historical_max_users_not_aligned
    from prep
    left join
        usage_ping_subscription_smau_aggregate
        on usage_ping_subscription_smau_aggregate.dim_subscription_id
        = prep.dim_subscription_id
    left join
        product_usage_wave_1_3_aggregate
        on product_usage_wave_1_3_aggregate.dim_subscription_id
        = prep.dim_subscription_id

)


{{
    dbt_audit(
        cte_ref="final",
        created_by="@trevor31",
        updated_by="@jpeguero",
        created_date="2021-02-04",
        updated_date="2022-03-26",
    )
}}

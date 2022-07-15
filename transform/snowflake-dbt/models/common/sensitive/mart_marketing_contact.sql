-- ------------------------ Start of PQL logic: --------------------------
{{
    simple_cte(
        [
            ("marketing_contact", "dim_marketing_contact"),
            ("marketing_contact_order", "bdg_marketing_contact_order"),
            ("dim_namespace", "dim_namespace"),
            ("gitlab_dotcom_namespaces_source", "gitlab_dotcom_namespaces_source"),
            ("gitlab_dotcom_users_source", "gitlab_dotcom_users_source"),
            ("gitlab_dotcom_members_source", "gitlab_dotcom_members_source"),
            ("gitlab_dotcom_memberships", "gitlab_dotcom_memberships"),
            ("customers_db_charges_xf", "customers_db_charges_xf"),
            ("customers_db_trials", "customers_db_trials"),
            ("customers_db_leads", "customers_db_leads_source"),
            (
                "gitlab_dotcom_daily_usage_data_events",
                "gitlab_dotcom_daily_usage_data_events",
            ),
            ("gitlab_dotcom_xmau_metrics", "gitlab_dotcom_xmau_metrics"),
            ("services", "gitlab_dotcom_services_source"),
            ("project", "prep_project"),
        ]
    )
}},
namespaces as (

    select
        gitlab_dotcom_users_source.email,
        dim_namespace.dim_namespace_id,
        dim_namespace.namespace_name,
        dim_namespace.created_at as namespace_created_at,
        dim_namespace.created_at::date as namespace_created_at_date,
        dim_namespace.gitlab_plan_title as plan_title,
        dim_namespace.creator_id,
        dim_namespace.current_member_count as member_count
    from dim_namespace
    left join
        gitlab_dotcom_users_source
        on gitlab_dotcom_users_source.user_id = dim_namespace.creator_id
    where
        dim_namespace.namespace_is_internal = false
        and lower(gitlab_dotcom_users_source.state) = 'active'
        and lower(dim_namespace.namespace_type) = 'group'
        and dim_namespace.ultimate_parent_namespace_id = dim_namespace.dim_namespace_id
        and date(dim_namespace.created_at) >= '2021-01-27'::date

),
flattened_members as (

    select  -- flattening members table to 1 record per member_id
        members.user_id,
        members.source_id,
        members.invite_created_at,
        min(members.invite_accepted_at) as invite_accepted_at
    from gitlab_dotcom_members_source members
    -- limit to just namespaces we care about
    -- same as namespace_id for group namespaces
    inner join namespaces on members.source_id = namespaces.dim_namespace_id
    where  -- only looking at namespace invites
        lower(members.member_source_type) = 'namespace'
        -- invite created after namespace created
        and members.invite_created_at >= namespaces.namespace_created_at
        -- invite accepted after invite created (removes weird edge cases with
        -- imported projects, etc)
        and ifnull(members.invite_accepted_at, current_timestamp)
        >= members.invite_created_at
        {{ dbt_utils.group_by(3) }}

),
invite_status as (

    select  -- pull in relevant namespace data, invite status, etc
        namespaces.dim_namespace_id,
        members.user_id,
        -- flag whether the user actually joined the namespace
        iff(memberships.user_id is not null, true, false) as invite_was_successful
    from flattened_members members
    join
        namespaces
        -- same as namespace_id for group namespaces
        on members.source_id = namespaces.dim_namespace_id
        -- this blocks namespaces created within two minutes of the namespace creator
        -- accepting their invite
        and (
            invite_accepted_at is null
            or (
                timestampdiff(minute, invite_accepted_at, namespace_created_at) not in (
                    0, 1, 2
                )
            )
        )
        = true
    left join  -- record added once invite is accepted/user has access
        gitlab_dotcom_memberships memberships
        on members.user_id = memberships.user_id
        and members.source_id = memberships.membership_source_id
        and memberships.is_billable = true
    -- not an "invite" if user created namespace
    where members.user_id != namespaces.creator_id

),
namespaces_with_user_count as (

    select dim_namespace_id, count(distinct user_id) as current_member_count
    from invite_status
    where invite_was_successful = true
    group by 1

),
subscriptions as (

    select
        charges.current_gitlab_namespace_id::int as namespace_id,
        min(charges.subscription_start_date) as min_subscription_start_date
    from customers_db_charges_xf charges
    inner join
        namespaces on charges.current_gitlab_namespace_id = namespaces.dim_namespace_id
    where
        charges.current_gitlab_namespace_id is not null
        -- changing to product category field, used by the charges table
        and charges.product_category in ('SaaS - Ultimate', 'SaaS - Premium')
    group by 1

),
latest_trial_by_user as (

    select *
    from customers_db_trials
    qualify
        row_number() over (partition by gitlab_user_id order by trial_start_date desc)
        = 1

),
pqls as (

    select distinct
        leads.product_interaction,
        leads.user_id,
        users.email,
        leads.namespace_id as dim_namespace_id,
        dim_namespace.namespace_name,
        leads.trial_start_date::date as trial_start_date,
        leads.created_at as pql_event_created_at
    from customers_db_leads leads
    left join gitlab_dotcom_users_source as users on leads.user_id = users.user_id
    left join dim_namespace on dim_namespace.dim_namespace_id = leads.namespace_id
    where lower(leads.product_interaction) = 'hand raise pql'

    union all

    select distinct
        leads.product_interaction,
        leads.user_id,
        users.email,
        latest_trial_by_user.gitlab_namespace_id as dim_namespace_id,
        dim_namespace.namespace_name,
        latest_trial_by_user.trial_start_date::date as trial_start_date,
        leads.created_at as pql_event_created_at
    from customers_db_leads as leads
    left join gitlab_dotcom_users_source as users on leads.user_id = users.user_id
    left join
        latest_trial_by_user on latest_trial_by_user.gitlab_user_id = leads.user_id
    left join dim_namespace on dim_namespace.dim_namespace_id = leads.namespace_id
    where
        lower(leads.product_interaction) = 'saas trial'
        and leads.is_for_business_use = 'True'

),
stages_adopted as (

    select
        namespaces.dim_namespace_id,
        namespaces.namespace_name,
        namespaces.email,
        namespaces.creator_id,
        namespaces.member_count,
        'SaaS Trial or Free' as product_interaction,
        subscriptions.min_subscription_start_date,
        arrayagg(distinct events.stage_name) as list_of_stages,
        count(distinct events.stage_name) as active_stage_count
    from gitlab_dotcom_daily_usage_data_events as events
    inner join namespaces on namespaces.dim_namespace_id = events.namespace_id
    left join
        gitlab_dotcom_xmau_metrics as xmau on xmau.events_to_include = events.event_name
    left join subscriptions on subscriptions.namespace_id = namespaces.dim_namespace_id
    where
        days_since_namespace_creation between 0 and 365
        -- Added in to only use events from a free or trial namespace (which filters
        -- based on the selection chose for the `free_or_trial` filter
        and events.plan_name_at_event_date in ('trial', 'free', 'ultimate_trial')
        and xmau.smau = true
        and events.event_date between namespaces.namespace_created_at_date and ifnull(
            subscriptions.min_subscription_start_date, current_date
        )
        {{ dbt_utils.group_by(7) }}

),
pqls_with_product_information as (

    select
        pqls.email,
        pqls.product_interaction as pql_product_interaction,
        coalesce(pqls.dim_namespace_id, stages_adopted.dim_namespace_id)::int
        as pql_namespace_id,
        coalesce(
            pqls.namespace_name, stages_adopted.namespace_name
        ) as pql_namespace_name_masked,
        pqls.user_id,
        pqls.trial_start_date as pql_trial_start_date,
        stages_adopted.min_subscription_start_date as pql_min_subscription_start_date,
        stages_adopted.list_of_stages as pql_list_stages,
        stages_adopted.active_stage_count as pql_nbr_stages,
        ifnull(namespaces_with_user_count.current_member_count, 0)
        + 1 as pql_nbr_namespace_users,
        pqls.pql_event_created_at
    from pqls
    left join stages_adopted on pqls.dim_namespace_id = stages_adopted.dim_namespace_id
    left join
        namespaces_with_user_count
        on namespaces_with_user_count.dim_namespace_id = pqls.dim_namespace_id
    where
        lower(pqls.product_interaction) = 'saas trial'
        and ifnull(stages_adopted.min_subscription_start_date, current_date)
        >= pqls.trial_start_date

    union all

    select
        pqls.email,
        pqls.product_interaction as pql_product_interaction,
        coalesce(pqls.dim_namespace_id, stages_adopted.dim_namespace_id)::int
        as pql_namespace_id,
        coalesce(
            pqls.namespace_name, stages_adopted.namespace_name
        ) as pql_namespace_name_masked,
        pqls.user_id,
        pqls.trial_start_date as pql_trial_start_date,
        stages_adopted.min_subscription_start_date as pql_min_subscription_start_date,
        stages_adopted.list_of_stages as pql_list_stages,
        stages_adopted.active_stage_count as pql_nbr_stages,
        ifnull(namespaces_with_user_count.current_member_count, 0)
        + 1 as pql_nbr_namespace_users,
        pqls.pql_event_created_at
    from pqls
    left join stages_adopted on pqls.dim_namespace_id = stages_adopted.dim_namespace_id
    left join
        namespaces_with_user_count
        on namespaces_with_user_count.dim_namespace_id = pqls.dim_namespace_id
    where lower(pqls.product_interaction) = 'hand raise pql'

),
latest_pql as (

    select
        pqls_with_product_information.*,
        gitlab_dotcom_namespaces_source.namespace_name as pql_namespace_name
    from pqls_with_product_information
    left join
        gitlab_dotcom_namespaces_source
        on gitlab_dotcom_namespaces_source.namespace_id
        = pqls_with_product_information.pql_namespace_id
    qualify
        row_number() over (partition by email order by pql_event_created_at desc) = 1

-- ------------------------ End of PQL logic --------------------------
),
services_by_marketing_contact_id as (

    select
        marketing_contact_order.dim_marketing_contact_id as dim_marketing_contact_id,
        count(*) as pql_nbr_integrations_installed,
        array_agg(
            distinct services.service_type) within group(order by services.service_type
        ) as pql_integrations_installed
    from services
    left join project on services.project_id = project.dim_project_id
    left join
        marketing_contact_order
        on marketing_contact_order.dim_namespace_id = project.dim_namespace_id
    group by 1

),
users_role_by_marketing_contact_id as (

    select
        marketing_contact_order.dim_marketing_contact_id,
        array_agg(
            distinct marketing_contact.job_title
        ) within group(order by marketing_contact.job_title
        ) as pql_namespace_creator_job_description
    from marketing_contact_order
    inner join
        dim_namespace
        on marketing_contact_order.dim_namespace_id = dim_namespace.dim_namespace_id
    inner join
        marketing_contact
        on dim_namespace.creator_id = marketing_contact.gitlab_dotcom_user_id
    group by 1

),
subscription_aggregate as (

    select
        dim_marketing_contact_id,
        min(subscription_start_date) as min_subscription_start_date,
        max(subscription_end_date) as max_subscription_end_date
    from marketing_contact_order
    where subscription_start_date is not null
    group by dim_marketing_contact_id

),
paid_subscription_aggregate as (

    select
        dim_marketing_contact_id,
        count(distinct dim_subscription_id) as nbr_of_paid_subscriptions
    from marketing_contact_order
    where
        dim_subscription_id is not null
        and (
            is_saas_bronze_tier
            or is_saas_premium_tier
            or is_saas_ultimate_tier
            or is_self_managed_starter_tier
            or is_self_managed_premium_tier
            or is_self_managed_ultimate_tier
        )
    group by dim_marketing_contact_id

),
distinct_contact_subscription as (

    select distinct
        dim_marketing_contact_id,
        dim_subscription_id,
        smau_manage_analytics_total_unique_counts_monthly,
        smau_plan_redis_hll_counters_issues_edit_issues_edit_total_unique_counts_monthly,
        smau_create_repo_writes,
        smau_verify_ci_pipelines_users_28_days,
        smau_package_redis_hll_counters_user_packages_user_packages_total_unique_counts_monthly,
        smau_release_release_creation_users_28_days,
        smau_configure_redis_hll_counters_terraform_p_terraform_state_api_unique_users_monthly,
        smau_monitor_incident_management_activer_user_28_days,
        smau_secure_secure_scanners_users_28_days,
        smau_protect_container_scanning_jobs_users_28_days,
        usage_umau_28_days_user,
        usage_action_monthly_active_users_project_repo_28_days_user,
        usage_merge_requests_28_days_user,
        usage_commit_comment_all_time_event,
        usage_source_code_pushes_all_time_event,
        usage_ci_pipelines_28_days_user,
        usage_ci_internal_pipelines_28_days_user,
        usage_ci_builds_28_days_user,
        usage_ci_builds_all_time_user,
        usage_ci_builds_all_time_event,
        usage_ci_runners_all_time_event,
        usage_auto_devops_enabled_all_time_event,
        usage_template_repositories_all_time_event,
        usage_ci_pipeline_config_repository_28_days_user,
        usage_user_unique_users_all_secure_scanners_28_days_user,
        usage_user_container_scanning_jobs_28_days_user,
        usage_user_sast_jobs_28_days_user,
        usage_user_dast_jobs_28_days_user,
        usage_user_dependency_scanning_jobs_28_days_user,
        usage_user_license_management_jobs_28_days_user,
        usage_user_secret_detection_jobs_28_days_user,
        usage_projects_with_packages_all_time_event,
        usage_projects_with_packages_28_days_user,
        usage_deployments_28_days_user,
        usage_releases_28_days_user,
        usage_epics_28_days_user,
        usage_issues_28_days_user,
        usage_instance_user_count_not_aligned,
        usage_historical_max_users_not_aligned
    from marketing_contact_order
    where dim_subscription_id is not null

),
usage_metrics as (

    select
        dim_marketing_contact_id,
        sum(
            smau_manage_analytics_total_unique_counts_monthly
        ) as smau_manage_analytics_total_unique_counts_monthly,
        sum(
            smau_plan_redis_hll_counters_issues_edit_issues_edit_total_unique_counts_monthly
        )
        as smau_plan_redis_hll_counters_issues_edit_issues_edit_total_unique_counts_monthly,
        sum(smau_create_repo_writes) as smau_create_repo_writes,
        sum(
            smau_verify_ci_pipelines_users_28_days
        ) as smau_verify_ci_pipelines_users_28_days,
        sum(
            smau_package_redis_hll_counters_user_packages_user_packages_total_unique_counts_monthly
        )
        as smau_package_redis_hll_counters_user_packages_user_packages_total_unique_counts_monthly,
        sum(
            smau_release_release_creation_users_28_days
        ) as smau_release_release_creation_users_28_days,
        sum(
            smau_configure_redis_hll_counters_terraform_p_terraform_state_api_unique_users_monthly
        )
        as smau_configure_redis_hll_counters_terraform_p_terraform_state_api_unique_users_monthly,
        sum(
            smau_monitor_incident_management_activer_user_28_days
        ) as smau_monitor_incident_management_activer_user_28_days,
        sum(
            smau_secure_secure_scanners_users_28_days
        ) as smau_secure_secure_scanners_users_28_days,
        sum(
            smau_protect_container_scanning_jobs_users_28_days
        ) as smau_protect_container_scanning_jobs_users_28_days,
        sum(usage_umau_28_days_user) as usage_umau_28_days_user,
        sum(
            usage_action_monthly_active_users_project_repo_28_days_user
        ) as usage_action_monthly_active_users_project_repo_28_days_user,
        sum(usage_merge_requests_28_days_user) as usage_merge_requests_28_days_user,
        sum(usage_commit_comment_all_time_event) as usage_commit_comment_all_time_event,
        sum(
            usage_source_code_pushes_all_time_event
        ) as usage_source_code_pushes_all_time_event,
        sum(usage_ci_pipelines_28_days_user) as usage_ci_pipelines_28_days_user,
        sum(
            usage_ci_internal_pipelines_28_days_user
        ) as usage_ci_internal_pipelines_28_days_user,
        sum(usage_ci_builds_28_days_user) as usage_ci_builds_28_days_user,
        sum(usage_ci_builds_all_time_user) as usage_ci_builds_all_time_user,
        sum(usage_ci_builds_all_time_event) as usage_ci_builds_all_time_event,
        sum(usage_ci_runners_all_time_event) as usage_ci_runners_all_time_event,
        sum(
            usage_auto_devops_enabled_all_time_event
        ) as usage_auto_devops_enabled_all_time_event,
        sum(
            usage_template_repositories_all_time_event
        ) as usage_template_repositories_all_time_event,
        sum(
            usage_ci_pipeline_config_repository_28_days_user
        ) as usage_ci_pipeline_config_repository_28_days_user,
        sum(
            usage_user_unique_users_all_secure_scanners_28_days_user
        ) as usage_user_unique_users_all_secure_scanners_28_days_user,
        sum(
            usage_user_container_scanning_jobs_28_days_user
        ) as usage_user_container_scanning_jobs_28_days_user,
        sum(usage_user_sast_jobs_28_days_user) as usage_user_sast_jobs_28_days_user,
        sum(usage_user_dast_jobs_28_days_user) as usage_user_dast_jobs_28_days_user,
        sum(
            usage_user_dependency_scanning_jobs_28_days_user
        ) as usage_user_dependency_scanning_jobs_28_days_user,
        sum(
            usage_user_license_management_jobs_28_days_user
        ) as usage_user_license_management_jobs_28_days_user,
        sum(
            usage_user_secret_detection_jobs_28_days_user
        ) as usage_user_secret_detection_jobs_28_days_user,
        sum(
            usage_projects_with_packages_all_time_event
        ) as usage_projects_with_packages_all_time_event,
        sum(
            usage_projects_with_packages_28_days_user
        ) as usage_projects_with_packages_28_days_user,
        sum(usage_deployments_28_days_user) as usage_deployments_28_days_user,
        sum(usage_releases_28_days_user) as usage_releases_28_days_user,
        sum(usage_epics_28_days_user) as usage_epics_28_days_user,
        sum(usage_issues_28_days_user) as usage_issues_28_days_user,
        sum(
            usage_instance_user_count_not_aligned
        ) as usage_instance_user_count_not_aligned,
        sum(
            usage_historical_max_users_not_aligned
        ) as usage_historical_max_users_not_aligned
    from distinct_contact_subscription
    group by dim_marketing_contact_id

),
prep as (

    select
        marketing_contact.dim_marketing_contact_id,
        case
            when
                max(
                    case
                        when
                            marketing_contact_order.marketing_contact_role
                            = 'Group Namespace Owner'
                        then 1
                        else 0
                    end
                )
                >= 1
            then true
            else false
        end as is_group_namespace_owner,
        case
            when
                max(
                    case
                        when
                            marketing_contact_order.marketing_contact_role
                            = 'Group Namespace Member'
                        then 1
                        else 0
                    end
                )
                >= 1
            then true
            else false
        end as is_group_namespace_member,
        case
            when
                max(
                    case
                        when
                            marketing_contact_order.marketing_contact_role
                            = 'Personal Namespace Owner'
                        then 1
                        else 0
                    end
                )
                >= 1
            then true
            else false
        end as is_individual_namespace_owner,
        case
            when
                max(
                    case
                        when
                            marketing_contact_order.marketing_contact_role
                            = 'Customer DB Owner'
                        then 1
                        else 0
                    end
                )
                >= 1
            then true
            else false
        end as is_customer_db_owner,
        case
            when
                max(
                    case
                        when
                            marketing_contact_order.marketing_contact_role
                            = 'Zuora Billing Contact'
                        then 1
                        else 0
                    end
                )
                >= 1
            then true
            else false
        end as is_zuora_billing_contact,
        min(
            marketing_contact_order.days_since_saas_trial_ended
        ) as days_since_saas_trial_ended,
        min(
            marketing_contact_order.days_since_saas_trial_ended_bucket
        ) as days_since_saas_trial_ended_bucket,
        max(
            marketing_contact_order.days_until_saas_trial_ends
        ) as days_until_saas_trial_ends,
        max(
            marketing_contact_order.days_until_saas_trial_ends_bucket
        ) as days_until_saas_trial_ends_bucket,
        case
            when
                max(
                    case
                        when marketing_contact_order.is_individual_namespace = 1
                        then marketing_contact_order.is_saas_trial
                        else null
                    end
                )
                >= 1
            then true
            else false
        end as individual_namespace_is_saas_trial,
        case
            when
                max(
                    case
                        when marketing_contact_order.is_individual_namespace = 1
                        then marketing_contact_order.is_saas_free_tier
                        else null
                    end
                )
                >= 1
            then true
            else false
        end as individual_namespace_is_saas_free_tier,
        case
            when
                max(
                    case
                        when marketing_contact_order.is_individual_namespace = 1
                        then marketing_contact_order.is_saas_bronze_tier
                        else null
                    end
                )
                >= 1
            then true
            else false
        end as individual_namespace_is_saas_bronze_tier,
        case
            when
                max(
                    case
                        when marketing_contact_order.is_individual_namespace = 1
                        then marketing_contact_order.is_saas_premium_tier
                        else null
                    end
                )
                >= 1
            then true
            else false
        end as individual_namespace_is_saas_premium_tier,
        case
            when
                max(
                    case
                        when marketing_contact_order.is_individual_namespace = 1
                        then marketing_contact_order.is_saas_ultimate_tier
                        else null
                    end
                )
                >= 1
            then true
            else false
        end as individual_namespace_is_saas_ultimate_tier,
        case
            when
                max(
                    case
                        when
                            marketing_contact_order.is_group_namespace = 1
                            and marketing_contact_order.marketing_contact_role
                            = 'Group Namespace Member'
                        then marketing_contact_order.is_saas_trial
                        else null
                    end
                )
                >= 1
            then true
            else false
        end as group_member_of_saas_trial,
        case
            when
                max(
                    case
                        when
                            marketing_contact_order.is_group_namespace = 1
                            and marketing_contact_order.marketing_contact_role
                            = 'Group Namespace Member'
                        then marketing_contact_order.is_saas_free_tier
                        else null
                    end
                )
                >= 1
            then true
            else false
        end as group_member_of_saas_free_tier,
        case
            when
                max(
                    case
                        when
                            marketing_contact_order.is_group_namespace = 1
                            and marketing_contact_order.marketing_contact_role
                            = 'Group Namespace Member'
                        then marketing_contact_order.is_saas_bronze_tier
                        else null
                    end
                )
                >= 1
            then true
            else false
        end as group_member_of_saas_bronze_tier,
        case
            when
                max(
                    case
                        when
                            marketing_contact_order.is_group_namespace = 1
                            and marketing_contact_order.marketing_contact_role
                            = 'Group Namespace Member'
                        then marketing_contact_order.is_saas_premium_tier
                        else null
                    end
                )
                >= 1
            then true
            else false
        end as group_member_of_saas_premium_tier,
        case
            when
                max(
                    case
                        when
                            marketing_contact_order.is_group_namespace = 1
                            and marketing_contact_order.marketing_contact_role
                            = 'Group Namespace Member'
                        then marketing_contact_order.is_saas_ultimate_tier
                        else null
                    end
                )
                >= 1
            then true
            else false
        end as group_member_of_saas_ultimate_tier,
        case
            when
                max(
                    case
                        when
                            marketing_contact_order.is_individual_namespace = 0
                            and marketing_contact_order.marketing_contact_role in (
                                'Group Namespace Owner'
                            )
                        then marketing_contact_order.is_saas_trial
                        else null
                    end
                )
                >= 1
            then true
            else false
        end as group_owner_of_saas_trial,
        case
            when
                max(
                    case
                        when
                            marketing_contact_order.is_individual_namespace = 0
                            and marketing_contact_order.marketing_contact_role in (
                                'Group Namespace Owner'
                            )
                        then marketing_contact_order.is_saas_free_tier
                        else null
                    end
                )
                >= 1
            then true
            else false
        end as group_owner_of_saas_free_tier,
        case
            when
                max(
                    case
                        when
                            marketing_contact_order.is_individual_namespace = 0
                            and marketing_contact_order.marketing_contact_role in (
                                'Group Namespace Owner'
                            )
                        then marketing_contact_order.is_saas_bronze_tier
                        else null
                    end
                )
                >= 1
            then true
            else false
        end as group_owner_of_saas_bronze_tier,
        case
            when
                max(
                    case
                        when
                            marketing_contact_order.is_individual_namespace = 0
                            and marketing_contact_order.marketing_contact_role in (
                                'Group Namespace Owner'
                            )
                        then marketing_contact_order.is_saas_premium_tier
                        else null
                    end
                )
                >= 1
            then true
            else false
        end as group_owner_of_saas_premium_tier,
        case
            when
                max(
                    case
                        when
                            marketing_contact_order.is_individual_namespace = 0
                            and marketing_contact_order.marketing_contact_role in (
                                'Group Namespace Owner'
                            )
                        then marketing_contact_order.is_saas_ultimate_tier
                        else null
                    end
                )
                >= 1
            then true
            else false
        end as group_owner_of_saas_ultimate_tier,
        case
            when
                max(
                    case
                        when
                            marketing_contact_order.is_individual_namespace = 0
                            and marketing_contact_order.marketing_contact_role in (
                                'Customer DB Owner', 'Zuora Billing Contact'
                            )
                        then marketing_contact_order.is_saas_trial
                        else null
                    end
                )
                >= 1
            then true
            else false
        end as responsible_for_group_saas_trial,
        case
            when
                max(
                    case
                        when
                            marketing_contact_order.is_individual_namespace = 0
                            and marketing_contact_order.marketing_contact_role in (
                                'Customer DB Owner', 'Zuora Billing Contact'
                            )
                        then marketing_contact_order.is_saas_free_tier
                        else null
                    end
                )
                >= 1
            then true
            else false
        end as responsible_for_group_saas_free_tier,
        case
            when
                max(
                    case
                        when
                            marketing_contact_order.is_individual_namespace = 0
                            and marketing_contact_order.marketing_contact_role in (
                                'Customer DB Owner', 'Zuora Billing Contact'
                            )
                        then marketing_contact_order.is_saas_bronze_tier
                        else null
                    end
                )
                >= 1
            then true
            else false
        end as responsible_for_group_saas_bronze_tier,
        case
            when
                max(
                    case
                        when
                            marketing_contact_order.is_individual_namespace = 0
                            and marketing_contact_order.marketing_contact_role in (
                                'Customer DB Owner', 'Zuora Billing Contact'
                            )
                        then marketing_contact_order.is_saas_premium_tier
                        else null
                    end
                )
                >= 1
            then true
            else false
        end as responsible_for_group_saas_premium_tier,
        case
            when
                max(
                    case
                        when
                            marketing_contact_order.is_individual_namespace = 0
                            and marketing_contact_order.marketing_contact_role in (
                                'Customer DB Owner', 'Zuora Billing Contact'
                            )
                        then marketing_contact_order.is_saas_ultimate_tier
                        else null
                    end
                )
                >= 1
            then true
            else false
        end as responsible_for_group_saas_ultimate_tier,
        case
            when max(marketing_contact_order.is_self_managed_starter_tier) >= 1
            then true
            else false
        end as is_self_managed_starter_tier,
        case
            when max(marketing_contact_order.is_self_managed_premium_tier) >= 1
            then true
            else false
        end as is_self_managed_premium_tier,
        case
            when max(marketing_contact_order.is_self_managed_ultimate_tier) >= 1
            then true
            else false
        end as is_self_managed_ultimate_tier,
        case
            when max(marketing_contact_order.is_setup_for_company) = true
            then true
            else false
        end as has_namespace_setup_for_company_use,
        case
            when max(marketing_contact_order.does_namespace_have_public_project) = true
            then true
            else false
        end as has_namespace_with_public_project,
        case
            when
                max(marketing_contact_order.does_free_namespace_have_public_project)
                = true
            then true
            else false
        end as has_free_namespace_with_public_project,
        array_agg(
            distinct ifnull(
                marketing_contact_order.marketing_contact_role
                || ': ' || ifnull(
                    marketing_contact_order.saas_product_tier, ''
                ) || ifnull(
                    marketing_contact_order.self_managed_product_tier, ''
                ),
                'No Role'
            )
        ) as role_tier_text,
        array_agg(
            distinct ifnull(
                marketing_contact_order.marketing_contact_role
                || ': ' || ifnull(
                    marketing_contact_order.namespace_path,
                    case
                        when
                            marketing_contact_order.self_managed_product_tier
                            is not null
                        then 'Self-Managed'
                        else ''
                    end
                )
                || ' | ' || ifnull(
                    marketing_contact_order.saas_product_tier, ''
                ) || ifnull(
                    marketing_contact_order.self_managed_product_tier, ''
                ),
                'No Namespace'
            )
        ) as role_tier_namespace_text

    from marketing_contact
    left join
        marketing_contact_order
        on marketing_contact_order.dim_marketing_contact_id
        = marketing_contact.dim_marketing_contact_id
    group by marketing_contact.dim_marketing_contact_id

),
joined as (

    select
        prep.*,
        iff(
            individual_namespace_is_saas_bronze_tier
            or group_owner_of_saas_bronze_tier
            or group_member_of_saas_bronze_tier
            or responsible_for_group_saas_bronze_tier,
            true,
            false
        ) as is_saas_bronze_tier,
        iff(
            individual_namespace_is_saas_premium_tier
            or group_owner_of_saas_premium_tier
            or group_member_of_saas_premium_tier
            or responsible_for_group_saas_premium_tier,
            true,
            false
        ) as is_saas_premium_tier,
        iff(
            individual_namespace_is_saas_ultimate_tier
            or group_owner_of_saas_ultimate_tier
            or group_member_of_saas_ultimate_tier
            or responsible_for_group_saas_ultimate_tier,
            true,
            false
        ) as is_saas_ultimate_tier,
        iff(
            is_saas_bronze_tier or is_self_managed_starter_tier, true, false
        ) as is_bronze_starter_tier,
        iff(
            is_saas_premium_tier or is_self_managed_premium_tier, true, false
        ) as is_premium_tier,
        iff(
            is_saas_ultimate_tier or is_self_managed_ultimate_tier, true, false
        ) as is_ultimate_tier,
        iff(
            is_saas_bronze_tier or is_saas_premium_tier or is_saas_ultimate_tier,
            true,
            false
        ) as is_saas_delivery,
        iff(
            is_self_managed_starter_tier
            or is_self_managed_premium_tier
            or is_self_managed_ultimate_tier,
            true,
            false
        ) as is_self_managed_delivery,
        iff(
            individual_namespace_is_saas_free_tier
            or group_member_of_saas_free_tier
            or group_owner_of_saas_free_tier,
            true,
            false
        ) as is_saas_free_tier,
        iff(is_saas_delivery or is_self_managed_delivery, true, false) as is_paid_tier,
        subscription_aggregate.min_subscription_start_date,
        subscription_aggregate.max_subscription_end_date,
        paid_subscription_aggregate.nbr_of_paid_subscriptions,
        case
            when
                (
                    prep.responsible_for_group_saas_free_tier
                    or prep.individual_namespace_is_saas_free_tier
                    or prep.group_owner_of_saas_free_tier
                )
                and not (
                    prep.responsible_for_group_saas_ultimate_tier
                    or prep.responsible_for_group_saas_premium_tier
                    or prep.responsible_for_group_saas_bronze_tier
                    or prep.individual_namespace_is_saas_bronze_tier
                    or prep.individual_namespace_is_saas_premium_tier
                    or prep.individual_namespace_is_saas_ultimate_tier
                    or prep.group_owner_of_saas_bronze_tier
                    or prep.group_owner_of_saas_premium_tier
                    or prep.group_owner_of_saas_ultimate_tier
                )
            then true
            else false
        end as responsible_for_free_tier_only,
        marketing_contact.email_address,
        marketing_contact.first_name,
        ifnull(marketing_contact.last_name, 'Unknown') as last_name,
        marketing_contact.gitlab_user_name,
        ifnull(marketing_contact.company_name, 'Unknown') as company_name,
        marketing_contact.sfdc_record_id,
        marketing_contact.dim_crm_account_id,
        marketing_contact.job_title,
        marketing_contact.it_job_title_hierarchy,
        marketing_contact.country,
        marketing_contact.mobile_phone,
        marketing_contact.sfdc_parent_sales_segment,
        marketing_contact.sfdc_parent_crm_account_tsp_region,
        marketing_contact.is_marketo_lead,
        marketing_contact.is_marketo_email_hard_bounced,
        marketing_contact.marketo_email_hard_bounced_date,
        marketing_contact.is_marketo_opted_out,
        marketing_contact.marketo_compliance_segment_value,
        marketing_contact.is_sfdc_lead_contact,
        marketing_contact.sfdc_lead_contact,
        marketing_contact.sfdc_created_date,
        marketing_contact.is_sfdc_opted_out,
        marketing_contact.is_gitlab_dotcom_user,
        marketing_contact.gitlab_dotcom_user_id,
        marketing_contact.gitlab_dotcom_created_date,
        marketing_contact.gitlab_dotcom_confirmed_date,
        marketing_contact.gitlab_dotcom_active_state,
        marketing_contact.gitlab_dotcom_last_login_date,
        marketing_contact.gitlab_dotcom_email_opted_in,
        marketing_contact.days_since_saas_signup,
        marketing_contact.days_since_saas_signup_bucket,
        marketing_contact.is_customer_db_user,
        marketing_contact.customer_db_customer_id,
        marketing_contact.customer_db_created_date,
        marketing_contact.customer_db_confirmed_date,
        iff(latest_pql.email is not null, true, false) as is_pql,
        latest_pql.pql_namespace_id,
        latest_pql.pql_namespace_name,
        latest_pql.pql_namespace_name_masked,
        latest_pql.pql_product_interaction,
        latest_pql.pql_list_stages,
        latest_pql.pql_nbr_stages,
        latest_pql.pql_nbr_namespace_users,
        latest_pql.pql_trial_start_date,
        latest_pql.pql_min_subscription_start_date,
        latest_pql.pql_event_created_at,
        services_by_marketing_contact_id.pql_nbr_integrations_installed,
        services_by_marketing_contact_id.pql_integrations_installed,
        users_role_by_marketing_contact_id.pql_namespace_creator_job_description,
        marketing_contact.days_since_self_managed_owner_signup,
        marketing_contact.days_since_self_managed_owner_signup_bucket,
        marketing_contact.zuora_contact_id,
        marketing_contact.zuora_created_date,
        marketing_contact.zuora_active_state,
        marketing_contact.wip_is_valid_email_address,
        marketing_contact.wip_invalid_email_address_reason,
        usage_metrics.smau_manage_analytics_total_unique_counts_monthly,
        usage_metrics.smau_plan_redis_hll_counters_issues_edit_issues_edit_total_unique_counts_monthly,
        usage_metrics.smau_create_repo_writes,
        usage_metrics.smau_verify_ci_pipelines_users_28_days,
        usage_metrics.smau_package_redis_hll_counters_user_packages_user_packages_total_unique_counts_monthly,
        usage_metrics.smau_release_release_creation_users_28_days,
        usage_metrics.smau_configure_redis_hll_counters_terraform_p_terraform_state_api_unique_users_monthly,
        usage_metrics.smau_monitor_incident_management_activer_user_28_days,
        usage_metrics.smau_secure_secure_scanners_users_28_days,
        usage_metrics.smau_protect_container_scanning_jobs_users_28_days,
        usage_metrics.usage_umau_28_days_user,
        usage_metrics.usage_action_monthly_active_users_project_repo_28_days_user,
        usage_metrics.usage_merge_requests_28_days_user,
        usage_metrics.usage_commit_comment_all_time_event,
        usage_metrics.usage_source_code_pushes_all_time_event,
        usage_metrics.usage_ci_pipelines_28_days_user,
        usage_metrics.usage_ci_internal_pipelines_28_days_user,
        usage_metrics.usage_ci_builds_28_days_user,
        usage_metrics.usage_ci_builds_all_time_user,
        usage_metrics.usage_ci_builds_all_time_event,
        usage_metrics.usage_ci_runners_all_time_event,
        usage_metrics.usage_auto_devops_enabled_all_time_event,
        usage_metrics.usage_template_repositories_all_time_event,
        usage_metrics.usage_ci_pipeline_config_repository_28_days_user,
        usage_metrics.usage_user_unique_users_all_secure_scanners_28_days_user,
        usage_metrics.usage_user_container_scanning_jobs_28_days_user,
        usage_metrics.usage_user_sast_jobs_28_days_user,
        usage_metrics.usage_user_dast_jobs_28_days_user,
        usage_metrics.usage_user_dependency_scanning_jobs_28_days_user,
        usage_metrics.usage_user_license_management_jobs_28_days_user,
        usage_metrics.usage_user_secret_detection_jobs_28_days_user,
        usage_metrics.usage_projects_with_packages_all_time_event,
        usage_metrics.usage_projects_with_packages_28_days_user,
        usage_metrics.usage_deployments_28_days_user,
        usage_metrics.usage_releases_28_days_user,
        usage_metrics.usage_epics_28_days_user,
        usage_metrics.usage_issues_28_days_user,
        usage_metrics.usage_instance_user_count_not_aligned,
        usage_metrics.usage_historical_max_users_not_aligned,
        'Raw' as lead_status,
        'Snowflake Email Marketing Database' as lead_source
    from prep
    left join
        marketing_contact
        on marketing_contact.dim_marketing_contact_id = prep.dim_marketing_contact_id
    left join
        subscription_aggregate
        on subscription_aggregate.dim_marketing_contact_id
        = marketing_contact.dim_marketing_contact_id
    left join
        paid_subscription_aggregate
        on paid_subscription_aggregate.dim_marketing_contact_id
        = marketing_contact.dim_marketing_contact_id
    left join
        usage_metrics
        on usage_metrics.dim_marketing_contact_id = prep.dim_marketing_contact_id
    left join
        services_by_marketing_contact_id
        on services_by_marketing_contact_id.dim_marketing_contact_id
        = marketing_contact.dim_marketing_contact_id
    left join latest_pql on latest_pql.email = marketing_contact.email_address
    left join
        users_role_by_marketing_contact_id
        on users_role_by_marketing_contact_id.dim_marketing_contact_id
        = marketing_contact.dim_marketing_contact_id
)

{{
    hash_diff(
        cte_ref="joined",
        return_cte="final",
        columns=[
            "is_group_namespace_owner",
            "is_group_namespace_member",
            "is_individual_namespace_owner",
            "is_customer_db_owner",
            "is_zuora_billing_contact",
            "days_since_saas_trial_ended",
            "days_until_saas_trial_ends",
            "individual_namespace_is_saas_trial",
            "individual_namespace_is_saas_free_tier",
            "individual_namespace_is_saas_bronze_tier",
            "individual_namespace_is_saas_premium_tier",
            "individual_namespace_is_saas_ultimate_tier",
            "group_member_of_saas_trial",
            "group_member_of_saas_free_tier",
            "group_member_of_saas_bronze_tier",
            "group_member_of_saas_premium_tier",
            "group_member_of_saas_ultimate_tier",
            "group_owner_of_saas_trial",
            "group_owner_of_saas_free_tier",
            "group_owner_of_saas_bronze_tier",
            "group_owner_of_saas_premium_tier",
            "group_owner_of_saas_ultimate_tier",
            "responsible_for_group_saas_trial",
            "responsible_for_group_saas_free_tier",
            "responsible_for_group_saas_bronze_tier",
            "responsible_for_group_saas_premium_tier",
            "responsible_for_group_saas_ultimate_tier",
            "is_self_managed_starter_tier",
            "is_self_managed_premium_tier",
            "is_self_managed_ultimate_tier",
            "min_subscription_start_date",
            "max_subscription_end_date",
            "nbr_of_paid_subscriptions",
            "email_address",
            "first_name",
            "last_name",
            "gitlab_user_name",
            "company_name",
            "job_title",
            "country",
            "sfdc_parent_sales_segment",
            "is_sfdc_lead_contact",
            "sfdc_lead_contact",
            "sfdc_created_date",
            "is_sfdc_opted_out",
            "is_gitlab_dotcom_user",
            "gitlab_dotcom_user_id",
            "gitlab_dotcom_created_date",
            "gitlab_dotcom_confirmed_date",
            "gitlab_dotcom_active_state",
            "gitlab_dotcom_last_login_date",
            "gitlab_dotcom_email_opted_in",
            "is_customer_db_user",
            "customer_db_customer_id",
            "customer_db_created_date",
            "customer_db_confirmed_date",
            "zuora_contact_id",
            "zuora_created_date",
            "zuora_active_state",
            "pql_list_stages",
            "pql_nbr_stages",
            "pql_nbr_namespace_users",
            "wip_is_valid_email_address",
            "wip_invalid_email_address_reason",
            "smau_manage_analytics_total_unique_counts_monthly",
            "smau_plan_redis_hll_counters_issues_edit_issues_edit_total_unique_counts_monthly",
            "smau_create_repo_writes",
            "smau_verify_ci_pipelines_users_28_days",
            "smau_package_redis_hll_counters_user_packages_user_packages_total_unique_counts_monthly",
            "smau_release_release_creation_users_28_days",
            "smau_configure_redis_hll_counters_terraform_p_terraform_state_api_unique_users_monthly",
            "smau_monitor_incident_management_activer_user_28_days",
            "smau_secure_secure_scanners_users_28_days",
            "smau_protect_container_scanning_jobs_users_28_days",
            "usage_umau_28_days_user",
            "usage_action_monthly_active_users_project_repo_28_days_user",
            "usage_merge_requests_28_days_user",
            "usage_commit_comment_all_time_event",
            "usage_source_code_pushes_all_time_event",
            "usage_ci_pipelines_28_days_user",
            "usage_ci_internal_pipelines_28_days_user",
            "usage_ci_builds_28_days_user",
            "usage_ci_builds_all_time_user",
            "usage_ci_builds_all_time_event",
            "usage_ci_runners_all_time_event",
            "usage_auto_devops_enabled_all_time_event",
            "usage_template_repositories_all_time_event",
            "usage_ci_pipeline_config_repository_28_days_user",
            "usage_user_unique_users_all_secure_scanners_28_days_user",
            "usage_user_container_scanning_jobs_28_days_user",
            "usage_user_sast_jobs_28_days_user",
            "usage_user_dast_jobs_28_days_user",
            "usage_user_dependency_scanning_jobs_28_days_user",
            "usage_user_license_management_jobs_28_days_user",
            "usage_user_secret_detection_jobs_28_days_user",
            "usage_projects_with_packages_all_time_event",
            "usage_projects_with_packages_28_days_user",
            "usage_deployments_28_days_user",
            "usage_releases_28_days_user",
            "usage_epics_28_days_user",
            "usage_issues_28_days_user",
            "usage_instance_user_count_not_aligned",
            "usage_historical_max_users_not_aligned",
            "has_namespace_setup_for_company_use",
            "pql_namespace_id",
            "pql_namespace_name",
            "pql_nbr_integrations_installed",
            "pql_integrations_installed",
            "pql_namespace_creator_job_description",
        ],
    )
}}

{{
    dbt_audit(
        cte_ref="final",
        created_by="@trevor31",
        updated_by="@jpeguero",
        created_date="2021-02-09",
        updated_date="2022-03-26",
    )
}}

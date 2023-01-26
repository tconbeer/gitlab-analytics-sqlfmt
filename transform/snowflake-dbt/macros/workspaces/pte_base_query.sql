{%- macro pte_base_query(model_run_type) -%}

-- Can only be set using days at the moment
-- {% set period_type = 'days'%}
{% set delta_value = 90 %}
-- Prediction date offset by -1 to ensure its only predicting with complete days.
{% set prediction_date = (
    modules.datetime.datetime.now() - modules.datetime.timedelta(days=1)
).date() %}

{%- if model_run_type == "training" -%}
{% set end_date = (
    prediction_date - modules.datetime.timedelta(days=delta_value)
).date() %}
{% endif %}
{% if model_run_type == "scoring" %} {% set end_date = prediction_date %} {% endif %}

-- Snapshot for just the "current" ARR month based on SNAPSHOT_DT
with
    mart_arr_snapshot_bottom_up as (

        select * from {{ ref("mart_arr_snapshot_bottom_up") }}

    ),
    period_1 as (

        select
            dim_crm_account_id,
            count(dim_subscription_id) as num_of_subs,
            max(crm_account_tsp_region) as crm_account_tsp_region,
            max(parent_crm_account_sales_segment) as parent_crm_account_sales_segment,
            max(parent_crm_account_industry) as parent_crm_account_industry,
            max(
                parent_crm_account_billing_country
            ) as parent_crm_account_billing_country,
            max(parent_crm_account_owner_team) as parent_crm_account_owner_team,
            max(
                case
                    when parent_crm_account_sales_territory != 'Territory Not Found'
                    then parent_crm_account_sales_territory
                end
            ) as parent_crm_account_sales_territory,
            max(parent_crm_account_tsp_region) as parent_crm_account_tsp_region,
            max(parent_crm_account_tsp_sub_region) as parent_crm_account_tsp_sub_region,
            max(parent_crm_account_tsp_area) as parent_crm_account_tsp_area,
            max(
                parent_crm_account_tsp_account_employees
            ) as crm_account_tsp_account_employees,
            max(
                parent_crm_account_tsp_max_family_employees
            ) as parent_crm_account_tsp_max_family_employees,
            max(
                parent_crm_account_employee_count_band
            ) as parent_crm_account_employee_count_band,
            max(
                case when product_tier_name like '%Ultimate%' then 1 else 0 end
            ) as is_ultimate_product_tier,
            max(
                case when product_tier_name like '%Premium%' then 1 else 0 end
            ) as is_premium_product_tier,
            max(
                case
                    when
                        product_tier_name like '%Starter%'
                        or product_tier_name like '%Bronze%'
                    then 1
                    else 0
                end
            ) as is_starter_bronze_product_tier,
            max(
                case when service_type = 'Full Service' then 1 else 0 end
            ) as is_service_type_full_service,
            max(
                case when service_type = 'Support Only' then 1 else 0 end
            ) as is_service_type_support_only,
            min(
                case
                    when term_start_date <= '{{ end_date }}'
                    then datediff(month, term_start_date, '{{ end_date }}')
                    else -1
                end
            ) as subscription_months_into,
            min(
                datediff(month, '{{ end_date }}', term_end_date)
            ) as subscription_months_remaining,
            min(
                datediff(month, subscription_start_date, subscription_end_date)
            ) as subscription_duration_in_months,
            min(
                datediff(month, parent_account_cohort_month, '{{ end_date }}')
            ) as account_tenure_in_months,
            avg(health_number) as health_number,
            sum(mrr) as sum_mrr,
            sum(arr) as sum_arr,
            sum(quantity) as license_count,
            sum(
                case when product_delivery_type = 'Self-Managed' then 1 else 0 end
            ) as self_managed_instance_count,
            sum(
                case when product_delivery_type = 'SaaS' then 1 else 0 end
            ) as saas_instance_count,
            sum(
                case when product_delivery_type = 'Others' then 1 else 0 end
            ) as others_instance_count,
            count(distinct(product_tier_name)) as num_products_purchased,
            sum(
                case
                    when
                        subscription_status = 'Cancelled'
                        or (
                            subscription_status = 'Active'
                            and subscription_end_date
                            <= dateadd(month, -3, '{{ end_date }}')
                        )
                    then 1
                    else 0
                end
            )
            -- added 3 months before counting active subscriptions as cancelled per
            -- Israel's feedback
            as cancelled_subs
        from mart_arr_snapshot_bottom_up
        -- Contains true-up snapshots for every date from 2020-03-01 to Present.
        -- MART_ARR_SNAPSHOT_MODEL contained non-true-up data but contains misisng
        -- data prior to 2021-06
        where
            snapshot_date
            -- limit to snapshot to day before our prediction window
            = '{{ end_date }}'
            and arr_month = date_trunc(
                'MONTH', cast('{{ end_date }}' as date)
            -- limit data for just the month the '{{ end_date }}' falls in. arr_month
            -- is unique at the dim_crm_account_id & snapshot_date level
            )
            and is_jihu_account
            -- Remove Chinese accounts like this per feedback from Melia and Israel
            != 'TRUE'
            and subscription_end_date
            -- filter to just active subscriptions per feedback by Melia
            >= '{{ end_date }}'
        group by
            -- dim_crm_account_id is not unique at each snapshot date, hence the group
            -- by
            dim_crm_account_id

    ),
    target as (

        select
            dim_crm_account_id,
            max(sum_arr)
            -- Provides the maximum ARR that account reached during our prediction
            -- window.
            as future_arr
        -- For accounts with multiple subscriptions we first have to sum their ARR to
        -- the arr_month level
        from
            (
                select dim_crm_account_id, arr_month, sum(arr) as sum_arr
                from
                    -- Contains Snapshot for every date from 2020-03-01 to Present
                    prod.restricted_safe_common_mart_sales.mart_arr_snapshot_bottom_up
                where
                    snapshot_date = '{{ prediction_date }}'
                    and arr_month > '{{ end_date }}'
                    and arr_month <= '{{ prediction_date }}'
                group by dim_crm_account_id, arr_month
            )

        group by dim_crm_account_id

    -- Snapshot for the set period prior to the "current" month (as specified by
    -- SNAPSHOT_DT).
    ),
    period_2 as (

        select
            dim_crm_account_id,
            count(dim_subscription_id) as num_of_subs_prev,
            max(
                parent_crm_account_tsp_account_employees
            ) as crm_account_tsp_account_employees_prev,
            sum(mrr) as sum_mrr_prev,
            sum(arr) as sum_arr_prev,
            sum(quantity) as license_count_prev,
            sum(
                case when product_delivery_type = 'Self-Managed' then 1 else 0 end
            ) as self_managed_instance_count_prev,
            sum(
                case when product_delivery_type = 'SaaS' then 1 else 0 end
            ) as saas_instance_count_prev,
            sum(
                case when product_delivery_type = 'Others' then 1 else 0 end
            ) as others_instance_count_prev,
            max(
                case when product_tier_name like '%Ultimate%' then 1 else 0 end
            ) as is_ultimate_product_tier_prev,
            max(
                case when product_tier_name like '%Premium%' then 1 else 0 end
            ) as is_premium_product_tier_prev,
            max(
                case
                    when
                        product_tier_name like '%Starter%'
                        or product_tier_name like '%Bronze%'
                    then 1
                    else 0
                end
            ) as is_starter_bronze_product_tier_prev,
            max(
                case when service_type = 'Full Service' then 1 else 0 end
            ) as is_service_type_full_service_prev,
            max(
                case when service_type = 'Support Only' then 1 else 0 end
            ) as is_service_type_support_only_prev,
            sum(
                case
                    when
                        subscription_status = 'Cancelled'
                        or (
                            subscription_status = 'Active'
                            and subscription_end_date
                            <= dateadd(month, -3, '{{ end_date }}')
                        )
                    then 1
                    else 0
                end
            )
            -- added 3 months before counting active subscriptions as cancelled per
            -- Israel's feedback
            as cancelled_subs_prev
        from mart_arr_snapshot_bottom_up
        where
            snapshot_date
            -- limit to snapshot to day before our prediction window
            = '{{ end_date }}'
            and arr_month = date_trunc(
                'MONTH',
                dateadd('{{ period_type }}', -365, cast('{{ end_date }}' as date))
            -- limit to customer's data for just the PERIOD prior to where the '{{
            -- end_date }}' falls
            )
            and is_jihu_account
            -- Remove Chinese accounts like this per feedback from Melia and Israel
            != 'TRUE'
        group by dim_crm_account_id

    -- Any metrics you would want to calculate the require multiple ARR_MONTHS. Could
    -- be lifetime metrics, over the last year, future expected ARR. etc.
    /*
), lifetime AS (

    SELECT dim_crm_account_id
           , COUNT(DISTINCT dim_subscription_id) AS num_of_subs_lifetime
           , AVG(DATEDIFF(month, subscription_start_month, subscription_end_month)) AS subscription_duration_in_months_lifetime
           --not added in period1 cte as it will always give 0
           , SUM(CASE WHEN subscription_status = 'Cancelled' THEN 1 ELSE 0 END) AS cancelled_subscriptions_lifetime
    FROM PROD.COMMON_MART_SALES.MART_ARR_SNAPSHOT_BOTTOM_UP -- Contains Snapshot for every date from 2020-03-01 to Present
    WHERE snapshot_date = $SNAPSHOT_DT -- limit to snapshot X periods prior to today
        AND is_jihu_account != 'TRUE' -- Remove Chinese accounts per Bhawana's notes, confirmed by Melia, removed missing values as per Israel's feedback
    GROUP BY dim_crm_account_id -- dim_crm_account_id is not unique at each snapshot date, hence the group by
*/
    -- SFDC Opportunity Table as it appears on the date of '{{ end_date }}'
    ),
    opps as (

        select
            account_id,
            count(distinct opportunity_id) as num_opportunities,
            sum(
                case when sales_path = 'Sales Assisted' then 1 else 0 end
            ) as sales_path_sales_assisted_cnt,
            sum(
                case when sales_path = 'Web Direct' then 1 else 0 end
            ) as sales_path_web_direct_cnt,
            sum(case when deal_size = 'Other' then 1 else 0 end) as deal_size_other_cnt,
            sum(
                case when deal_size = '1 - Small (<5k)' then 1 else 0 end
            ) as deal_size_small_cnt,
            sum(
                case when deal_size = '2 - Medium (5k - 25k)' then 1 else 0 end
            ) as deal_size_medium_cnt,
            sum(
                case when deal_size = '3 - Big (25k - 100k)' then 1 else 0 end
            ) as deal_size_big_cnt,
            sum(
                case when deal_size = '4 - Jumbo (>100k)' then 1 else 0 end
            ) as deal_size_jumbo_cnt,
            sum(
                case
                    when
                        stage_name in ('Closed Won')
                        and order_type_stamped != '7. PS / Other'
                    then 1
                    else 0
                end
            ) as won_opportunities,
            sum(
                case
                    when
                        stage_name in ('8-Closed Lost', 'Closed Lost')
                        and order_type_stamped != '7. PS / Other'
                    then 1
                    else 0
                end
            ) as lost_opportunities,
            count(
                case
                    when
                        order_type_stamped in ('2. New - Connected', '3. Growth')
                        and net_arr > 0
                        and stage_name in ('Closed Won')
                    then 1
                    else 0
                end
            ) as num_expansions,
            count(
                case
                    when
                        order_type_stamped in ('4. Contraction', '5. Churn - Partial')
                        and net_arr <= 0
                    then 1
                    else 0
                end
            ) as num_contractions,
            sum(
                case when sales_type = 'Renewal' then 1 else 0 end
            ) as num_opportunities_by_renewal,
            sum(
                case when order_type_stamped = '1. New - First Order' then 1 else 0 end
            ) as num_opportunities_new_business,
            sum(
                case when sales_type = 'Add-On Business' then 1 else 0 end
            ) as num_opportunities_add_on_business,
            sum(net_arr) as sum_net_arr,
            sum(
                case when stage_name in ('Closed Won') then net_arr else 0 end
            ) as sum_net_arr_won_opportunities,
            sum(
                case when stage_name in ('8-Closed Lost') then net_arr else 0 end
            ) as sum_net_arr_lost_opportunities,
            sum(
                case
                    when sales_type = 'Renewal' and stage_name in ('Closed Won')
                    then 1
                    else 0
                end
            ) as won_opportunities_by_renewal,
            sum(
                case
                    when
                        order_type_stamped = '1. New - First Order'
                        and stage_name in ('Closed Won')
                    then 1
                    else 0
                end
            ) as won_opportunities_new_business,
            sum(
                case
                    when sales_type = 'Add-On Business' and stage_name in ('Closed Won')
                    then 1
                    else 0
                end
            ) as won_opportunities_add_on_business,
            sum(
                case
                    when sales_type = 'Renewal' and stage_name in ('8-Closed Lost')
                    then 1
                    else 0
                end
            ) as lost_opportunities_by_renewal,
            sum(
                case
                    when
                        order_type_stamped = '1. New - First Order'
                        and stage_name in ('8-Closed Lost')
                    then 1
                    else 0
                end
            ) as lost_opportunities_new_business,
            sum(
                case
                    when
                        sales_type = 'Add-On Business'
                        and stage_name in ('8-Closed Lost')
                    then 1
                    else 0
                end
            ) as lost_opportunities_add_on_business,
            max(competitors_other_flag) as competitors_other,
            max(competitors_gitlab_core_flag) as competitors_gitlab_core,
            max(competitors_none_flag) as competitors_none,
            max(competitors_github_enterprise_flag) as competitors_github_enterprise,
            max(competitors_bitbucket_server_flag) as competitors_bitbucket_server,
            max(competitors_unknown_flag) as competitors_unknown,
            max(competitors_github_flag) as competitors_github,
            max(competitors_gitlab_flag) as competitors_gitlab,
            max(competitors_jenkins_flag) as competitors_jenkins,
            max(competitors_azure_devops_flag) as competitors_azure_devops,
            max(competitors_svn_flag) as competitors_svn,
            max(competitors_bitbucket_flag) as competitors_bitbucket,
            max(competitors_atlassian_flag) as competitors_atlassian,
            max(competitors_perforce_flag) as competitors_perforce,
            max(competitors_visual_studio_flag) as competitors_visual_studio,
            max(competitors_azure_flag) as competitors_azure,
            max(competitors_amazon_code_commit_flag) as competitors_amazon_code_commit,
            max(competitors_circleci_flag) as competitors_circleci,
            max(competitors_bamboo_flag) as competitors_bamboo,
            max(competitors_aws_flag) as competitors_aws,
            sum(
                case when cp_use_cases = 'CI: Automate build and test' then 1 else 0 end
            ) as use_case_continuous_integration,
            sum(
                case
                    when
                        cp_use_cases
                        = 'DevSecOps: Test for application security vulnerabilities early and often'
                    then 1
                    else 0
                end
            ) as use_case_dev_sec_ops,
            sum(
                case
                    when cp_use_cases = 'CD: Automate delivery and deployment'
                    then 1
                    else 0
                end
            ) as use_case_continuous_delivery,
            sum(
                case
                    when
                        cp_use_cases
                        = 'VCC: Collaborate and manage source code (SCM), designs, and more'
                    then 1
                    else 0
                end
            ) as use_case_version_controlled_configuration,
            sum(
                case
                    when
                        cp_use_cases
                        = 'Simplify DevOps: Manage and streamline my entire DevOps lifecycle'
                    then 1
                    else 0
                end
            ) as use_case_simplify_dev_ops,
            sum(
                case
                    when
                        cp_use_cases
                        = 'Agile: Improve how we iteratively plan, manage, and deliver projects'
                    then 1
                    else 0
                end
            ) as use_case_agile,
            sum(case when cp_use_cases = 'Other' then 1 else 0 end) as use_case_other,
            sum(
                case
                    when
                        cp_use_cases
                        = 'Cloud-Native: Embrace modern, cloud-native application development'
                    then 1
                    else 0
                end
            ) as use_case_cloud_native,
            sum(
                case
                    when
                        cp_use_cases
                        = 'GitOps: Automatically provision, manage and maintain infrastructure'
                    then 1
                    else 0
                end
            ) as use_case_git_ops
        from {{ ref("wk_sales_sfdc_opportunity_snapshot_history_xf") }}
        where
            snapshot_date = '{{ end_date }}'
            -- filter as requested by Noel
            and opportunity_category in ('Standard', 'Decommissioned', 'Ramp Deal')
            and created_date between dateadd(
                '{{ period_type }}', - '{{ delta_value }}', '{{ end_date }}'
            ) and '{{ end_date }}'
        group by account_id

    ),
    events_salesforce as (

        select
            account_id as account_id,
            sum(
                case when event_type = 'IQM' then 1 else 0 end
            ) as initial_qualifying_meeting_event_count,
            sum(
                case when event_type = 'Meeting' then 1 else 0 end
            ) as meeting_event_count,
            sum(
                case when event_type = 'Web Conference' then 1 else 0 end
            ) as web_conference_event_count,
            sum(case when event_type = 'Call' then 1 else 0 end) as call_event_count,
            sum(case when event_type = 'Demo' then 1 else 0 end) as demo_event_count,
            sum(
                case when event_type = 'In Person' then 1 else 0 end
            ) as in_person_event_count,
            sum(
                case when event_type = 'Renewal' then 1 else 0 end
            ) as renewal_event_count,
            sum(case when event_type is not null then 1 else 0 end) as total_event_count
        from {{ ref("sfdc_event_source") }}
        where
            created_at between dateadd(
                '{{ period_type }}', - '{{ delta_value }}', '{{ end_date }}'
            )
            -- filter PERIOD window. Because no histroic event table, going off
            -- createddate
            and '{{ end_date }}'
        group by account_id

    ),
    tasks_salesforce as (

        select
            accountid as account_id,
            sum(case when type = 'Email' then 1 else 0 end) as email_task_count,
            sum(case when type = 'Call' then 1 else 0 end) as call_task_count,
            sum(case when type = 'Demo' then 1 else 0 end) as demo_task_count,
            sum(
                case when type = 'Sales Alignment' then 1 else 0 end
            ) as sales_alignment_task_count,
            sum(case when type is not null then 1 else 0 end) as total_task_count,
            sum(is_answered__c) as is_answered_task,
            sum(is_busy__c) as is_busy_task,
            sum(is_correct_contact__c) as is_correct_contact_task,
            sum(is_left_message__c) as is_left_message_task,
            sum(is_not_answered__c) as is_not_answered_task
        from {{ source("salesforce", "task") }}
        where
            createddate between dateadd(
                '{{ period_type }}', - '{{ delta_value }}', '{{ end_date }}'
            )
            -- filter PERIOD window. Because no histroic task table, going on
            -- createddate
            and '{{ end_date }}'
        group by account_id

    ),
    zi_technologies as (

        select
            account_id_18__c as account_id,
            max(zi_revenue__c) as zi_revenue,
            max(zi_industry__c) as zi_industry,
            max(zi_sic_code__c) as zi_sic_code,
            max(zi_naics_code__c) as zi_naics_code,
            max(zi_number_of_developers__c) as zi_developers_cnt,
            -- , MAX(zi_products_and_services__c) AS zi_products_and_services --
            -- Leaving out for now but could be useful to parse later
            -- Atlassian
            max(
                case when contains(zi_technologies__c, 'ARE_USED: Atlassian') then 1 end
            ) as zi_atlassian_flag,
            max(
                case
                    when
                        (
                            contains(zi_technologies__c, 'ARE_USED: BitBucket')
                            or contains(
                                zi_technologies__c, 'ARE_USED: AtlASsian Bitbucket'
                            )
                        )
                    then 1
                end
            ) as zi_bitbucket_flag,
            max(
                case
                    when
                        contains(
                            zi_technologies__c, 'ARE_USED: Atlassian Jira Agile Tools'
                        )
                    then 1
                end
            ) as zi_jira_flag,
            -- GCP
            max(
                case
                    when
                        (
                            contains(
                                zi_technologies__c, 'ARE_USED: Google Cloud Platform'
                            )
                            or contains(zi_technologies__c, 'ARE_USED: GCP')
                        )
                    then 1
                end
            ) as zi_gcp_flag,
            -- Github
            max(
                case when contains(zi_technologies__c, 'ARE_USED: GitHub') then 1 end
            ) as zi_github_flag,
            max(
                case
                    when contains(zi_technologies__c, 'ARE_USED: GitHub Enterprise')
                    then 1
                end
            ) as zi_github_enterprise_flag,
            -- AWS
            max(
                case when contains(zi_technologies__c, 'ARE_USED: AWS') then 1 end
            ) as zi_aws_flag,
            max(
                case
                    when
                        (
                            contains(
                                zi_technologies__c,
                                'ARE_USED: Amazon AWS Identity and Access Management'
                            )
                            or contains(
                                zi_technologies__c,
                                'Amazon AWS Identity and Access Management (IAM)'
                            )
                        )
                    then 1
                end
            ) as zi_aws_iam_flag,
            max(
                case
                    when contains(zi_technologies__c, 'ARE_USED: Amazon AWS CloudTrail')
                    then 1
                end
            ) as zi_aws_cloud_trail_flag,
            -- Other CI
            max(
                case when contains(zi_technologies__c, 'ARE_USED: Hashicorp') then 1 end
            ) as zi_hashicorp_flag,
            max(
                case
                    when
                        (
                            contains(zi_technologies__c, 'ARE_USED: CircleCI')
                            or contains(
                                zi_technologies__c, 'ARE_USED: Circle Internet Services'
                            )
                        )
                    then 1
                end
            ) as zi_circleci_flag,
            max(
                case when contains(zi_technologies__c, 'ARE_USED: TravisCI') then 1 end
            ) as zi_travisci_flag,
            -- Open Source/Free
            max(
                case
                    when
                        (
                            contains(zi_technologies__c, 'ARE_USED: Apache Subversion')
                            or contains(zi_technologies__c, 'ARE_USED: SVN')
                        )
                    then 1
                end
            ) as zi_apache_subversion_flag,
            max(
                case when contains(zi_technologies__c, 'ARE_USED: Jenkins') then 1 end
            ) as zi_jenkins_flag,
            max(
                case
                    when contains(zi_technologies__c, 'ARE_USED: TortoiseSVN') then 1
                end
            ) as zi_tortoise_svn_flag,
            max(
                case
                    when contains(zi_technologies__c, 'ARE_USED: Kubernetes') then 1
                end
            ) as zi_kubernetes_flag
        from {{ source("snapshots", "sfdc_account_snapshots") }}
        -- Cast from datetime to date
        where cast(dbt_updated_at as date) = '{{ end_date }}'
        group by
            -- snapshots occur multiple times a day so data is not unique at the
            -- acccount + dbt_updated_at level.
            account_id

    ),
    bizible as (

        select
            dim_crm_account_id,
            count(dim_crm_touchpoint_id) as num_bizible_touchpoints,
            count(distinct dim_campaign_id) as num_campaigns,
            sum(
                case when bizible_touchpoint_source = 'Web Direct' then 1 else 0 end
            ) as touchpoint_source_web_direct,
            sum(
                case
                    when bizible_touchpoint_source = 'Organic - Google' then 1 else 0
                end
            ) as touchpoint_source_web_organic_google,
            sum(
                case when bizible_touchpoint_source = 'CRM Campaign' then 1 else 0 end
            ) as touchpoint_source_crm_campaign,
            sum(
                case when bizible_touchpoint_source = 'marketo' then 1 else 0 end
            ) as touchpoint_source_marketo,
            sum(
                case when bizible_touchpoint_source = 'CRM Activity' then 1 else 0 end
            ) as touchpoint_source_crm_activity,
            sum(
                case
                    when
                        bizible_touchpoint_source in (
                            'facebook',
                            'Facebook',
                            'facebook.com',
                            'linkedin',
                            'LinkedIn',
                            'linkedin_elevate',
                            'linkedin/',
                            'twitter',
                            'Twitter',
                            'twitter.com'
                        )
                    then 1
                    else 0
                end
            ) as touchpoint_source_social_media,
            sum(
                case when bizible_touchpoint_type = 'Web Form' then 1 else 0 end
            ) as touchpoint_type_web_form,
            sum(
                case when bizible_touchpoint_type = 'Web Visit' then 1 else 0 end
            ) as touchpoint_type_web_visit,
            sum(
                case when bizible_touchpoint_type = 'CRM' then 1 else 0 end
            ) as touchpoint_type_crm,
            sum(
                case when bizible_touchpoint_type = 'IQM' then 1 else 0 end
            ) as touchpoint_type_iqm,
            sum(
                case when bizible_touchpoint_type = 'Web Chat' then 1 else 0 end
            ) as touchpoint_type_web_chat,
            sum(
                case when bizible_marketing_channel = 'Direct' then 1 else 0 end
            ) as touchpoint_marketing_channel_direct,
            sum(
                case when bizible_marketing_channel = 'Organic Search' then 1 else 0 end
            ) as touchpoint_marketing_channel_organic_search,
            sum(
                case when bizible_marketing_channel = 'Email' then 1 else 0 end
            ) as touchpoint_marketing_channel_email,
            sum(
                case when bizible_marketing_channel = 'Web Referral' then 1 else 0 end
            ) as touchpoint_marketing_channel_web_referral,
            sum(
                case when bizible_marketing_channel = 'Event' then 1 else 0 end
            ) as touchpoint_marketing_channel_web_event,
            sum(
                case when type = 'Inbound Request' then 1 else 0 end
            ) as touchpoint_type_inbound_request,
            sum(
                case when type = 'Direct Mail' then 1 else 0 end
            ) as touchpoint_type_direct_mail,
            sum(case when type = 'Trial' then 1 else 0 end) as touchpoint_type_trial,
            sum(case when type = 'Webcast' then 1 else 0 end) as touchpoint_type_webcast
            ,
            sum(
                case when bizible_medium = 'Web' then 1 else 0 end
            ) as touchpoint_bizible_medium_web,
            sum(
                case when bizible_medium = 'Search' then 1 else 0 end
            ) as touchpoint_bizible_medium_search,
            sum(
                case when bizible_medium = 'email' then 1 else 0 end
            ) as touchpoint_bizible_medium_email,
            sum(
                case when bizible_medium = 'Trial' then 1 else 0 end
            ) as touchpoint_bizible_medium_trial,
            sum(
                case when bizible_medium = 'Webcast' then 1 else 0 end
            ) as touchpoint_bizible_medium_webcast,
            sum(
                case when crm_person_status = 'Qualified' then 1 else 0 end
            ) as touchpoint_crm_person_status_qualified,
            sum(
                case when crm_person_status = 'Inquery' then 1 else 0 end
            ) as touchpoint_crm_person_status_inquery,
            sum(
                case when crm_person_status = 'MQL' then 1 else 0 end
            ) as touchpoint_crm_person_status_mql,
            sum(
                case when crm_person_status = 'Nurture' then 1 else 0 end
            ) as touchpoint_crm_person_status_nurture,
            sum(
                case when crm_person_status = 'Qualifying' then 1 else 0 end
            ) as touchpoint_crm_person_status_qualifying,
            sum(
                case when crm_person_status = 'Accepted' then 1 else 0 end
            ) as touchpoint_crm_person_status_accepted,
            sum(
                case
                    when crm_person_title in ('CTO', 'Chief Technology Officer')
                    then 1
                    else 0
                end
            ) as touchpoint_crm_person_title_cto,
            sum(
                case when crm_person_title = 'Software Developer' then 1 else 0 end
            ) as touchpoint_crm_person_title_software_developer,
            sum(
                case when crm_person_title = 'Software Engineer' then 1 else 0 end
            ) as touchpoint_crm_person_title_software_engineer,
            sum(
                case when crm_person_title = 'Development Team Lead' then 1 else 0 end
            ) as touchpoint_crm_person_title_software_dev_team_lead
        from {{ ref("mart_crm_attribution_touchpoint") }}
        where
            bizible_touchpoint_date between dateadd(
                '{{ period_type }}', - '{{ delta_value }}', '{{ end_date }}'
            ) and '{{ end_date }}'
        group by dim_crm_account_id

    ),
    product_usage as (

        select
            dim_crm_account_id,
            avg(umau_28_days_user) as unique_active_user,
            avg(
                action_monthly_active_users_project_repo_28_days_user
            ) as action_monthly_active_users_project_repo_avg,
            avg(merge_requests_28_days_user) as merge_requests_avg,
            avg(
                projects_with_repositories_enabled_28_days_user
            ) as projects_with_repositories_enabled_avg,
            avg(ci_pipelines_28_days_user) as ci_pipelines_avg,
            avg(ci_internal_pipelines_28_days_user) as ci_internal_pipelines_avg,
            avg(ci_builds_28_days_user) as ci_builds_avg,
            avg(
                ci_pipeline_config_repository_28_days_user
            ) as ci_pipeline_config_repository_avg,
            avg(
                user_unique_users_all_secure_scanners_28_days_user
            ) as user_unique_users_all_secure_scanners_avg,
            avg(user_sast_jobs_28_days_user) as user_sast_jobs_avg,
            avg(user_dast_jobs_28_days_user) as user_dast_jobs_avg,
            avg(
                user_dependency_scanning_jobs_28_days_user
            ) as user_dependency_scanning_jobs_avg,
            avg(
                user_license_management_jobs_28_days_user
            ) as user_license_management_jobs_avg,
            avg(
                user_secret_detection_jobs_28_days_user
            ) as user_secret_detection_jobs_avg,
            avg(
                user_container_scanning_jobs_28_days_user
            ) as user_container_scanning_jobs_avg,
            avg(deployments_28_days_user) as deployments_avg,
            avg(releases_28_days_user) as releases_avg,
            avg(epics_28_days_user) as epics_avg,
            avg(issues_28_days_user) as issues_avg,
            avg(analytics_28_days_user) as analytics_avg,
            sum(
                case when instance_type = 'Production' then active_user_count end
            ) as active_user_count_cnt,
            sum(
                case when instance_type = 'Production' then license_user_count end
            ) as license_user_count_cnt,
            sum(
                case when instance_type = 'Production' then billable_user_count end
            ) as billable_user_count_cnt,
            max(commit_comment_all_time_event) as commit_comment_all_time_event,
            max(source_code_pushes_all_time_event) as source_code_pushes_all_time_event,
            max(
                template_repositories_all_time_event
            ) as template_repositories_all_time_event,
            max(ci_runners_all_time_event) as ci_runners_all_time_event,
            max(
                auto_devops_enabled_all_time_event
            ) as auto_devops_enabled_all_time_event,
            max(
                projects_with_packages_all_time_event
            ) as projects_with_packages_all_time_event,
            max(merge_requests_all_time_event) as merge_requests_all_time_event,
            max(epics_all_time_event) as epics_all_time_event,
            max(issues_all_time_event) as issues_all_time_event,
            max(projects_all_time_event) as projects_all_time_event,
            max(
                case when gitlab_shared_runners_enabled = 'TRUE' then 1 else 0 end
            ) as gitlab_shared_runners_enabled,
            max(
                case when container_registry_enabled = 'TRUE' then 1 else 0 end
            ) as container_registry_enabled,
            max(
                case
                    when instance_type = 'Production' then max_historical_user_count
                end
            ) as max_historical_user_count,
            max(
                cast(
                    substring(
                        cleaned_version, 0, charindex('.', cleaned_version) - 1
                    ) as int
                )
            ) as gitlab_version
        from {{ ref("mart_product_usage_paid_user_metrics_monthly") }}
        where
            ping_created_at is not null
            and snapshot_month between date_trunc(
                month,
                dateadd(
                    '{{ period_type }}', - '{{ delta_value }}',
                    cast('{{ end_date }}' as date)
                )
            ) and date_trunc(month, dateadd(month, -1, cast('{{ end_date }}' as date)))
        group by dim_crm_account_id

    )

-- This is the final output table that creates the modeling dataset
select
    p1.dim_crm_account_id as crm_account_id,
    -- Outcome variables
    case
        when
            coalesce(p1.sum_arr, 0) != 0
            and (
                (coalesce(t.future_arr, 0) - coalesce(p1.sum_arr, 0))
                / coalesce(p1.sum_arr, 0)
            )
            > 0.1
        then 1  -- If there is more than a 10% increase in ARR
        else 0
    end as is_expanded_flag,
    coalesce(t.future_arr, 0) - coalesce(p1.sum_arr, 0) as is_expanded_amt,
    -- Zuora Fields
    p1.num_of_subs as subs_cnt,
    p1.cancelled_subs as cancelled_subs_cnt,
    case
        when
            p1.crm_account_tsp_region = 'AMER'
            or p1.crm_account_tsp_region like 'AMER%'
            or p1.crm_account_tsp_region like 'US%'
        then 'AMER'
        when
            p1.crm_account_tsp_region = 'EMEA'
            or p1.crm_account_tsp_region like 'Germany%'
        then 'EMEA'
        when p1.crm_account_tsp_region is null
        then 'Unknown'
        else p1.crm_account_tsp_region
    end as account_region,
    -- , COALESCE(p1.crm_account_tsp_region, 'Unknown') AS account_region
    coalesce(p1.parent_crm_account_sales_segment, 'Unknown') as account_sales_segment,
    coalesce(p1.parent_crm_account_industry, 'Unknown') as account_industry,
    coalesce(
        p1.parent_crm_account_billing_country, 'Unknown'
    ) as account_billing_country,
    coalesce(p1.parent_crm_account_owner_team, 'Unknown') as account_owner_team,
    coalesce(
        p1.parent_crm_account_sales_territory, 'Unknown'
    ) as account_sales_territory,
    case
        when
            p1.parent_crm_account_tsp_region = 'AMER'
            or p1.parent_crm_account_tsp_region like 'AMER%'
            or p1.parent_crm_account_tsp_region like 'US%'
        then 'AMER'
        when
            p1.parent_crm_account_tsp_region = 'EMEA'
            or p1.parent_crm_account_tsp_region like 'Germany%'
        then 'EMEA'
        when p1.parent_crm_account_tsp_region is null
        then 'Unknown'
        else p1.parent_crm_account_tsp_region
    end as parent_account_region,
    -- , COALESCE(p1.parent_crm_account_tsp_region, 'Unknown') AS parent_account_region
    coalesce(
        p1.parent_crm_account_tsp_sub_region, 'Unknown'
    ) as parent_account_sub_region,
    coalesce(p1.parent_crm_account_tsp_area, 'Unknown') as parent_account_area,
    p1.crm_account_tsp_account_employees as parent_account_employees_cnt,
    p1.parent_crm_account_tsp_max_family_employees
    as parent_account_max_family_employees_cnt,
    p1.parent_crm_account_employee_count_band as parent_account_employee_count_band,
    p1.is_ultimate_product_tier as is_ultimate_product_tier_flag,
    p1.is_premium_product_tier as is_premium_product_tier_flag,
    p1.is_starter_bronze_product_tier as is_starter_bronze_product_tier_flag,
    p1.is_service_type_full_service as is_service_type_full_service_flag,
    p1.is_service_type_support_only as is_service_type_support_only_flag,
    p1.subscription_months_into as subscription_months_into,
    p1.subscription_months_remaining as subscription_months_remaining,
    p1.subscription_duration_in_months as subscription_duration_in_months,
    p1.account_tenure_in_months as account_tenure_in_months,
    p1.health_number as health_number,
    coalesce(p1.sum_mrr, 0) as mrr_amt,
    coalesce(p1.sum_arr, 0) as arr_amt,
    coalesce(p1.license_count, 0) as license_cnt,
    p1.sum_arr / p1.license_count as arpu,
    coalesce(p1.self_managed_instance_count, 0) as self_managed_instance_cnt,
    coalesce(p1.saas_instance_count, 0) as saas_instance_cnt,
    coalesce(p1.others_instance_count, 0) as others_instance_cnt,
    coalesce(p1.num_products_purchased, 0) as products_purchased_cnt,
    -- Previous Period Zuora Fields
    coalesce(p2.sum_arr_prev, 0) as arr_prev_amt,
    coalesce(p2.sum_mrr_prev, 0) as mrr_prev_amt,
    coalesce(p2.cancelled_subs_prev, 0) as cancelled_subs_prev_cnt,
    coalesce(p2.num_of_subs_prev, 0) as subs_prev_cnt,
    coalesce(
        crm_account_tsp_account_employees_prev, 0
    ) as crm_account_tsp_account_employees_prev_cnt,
    coalesce(license_count_prev, 0) as license_prev_cnt,
    -- Zuora Change Fields
    case
        when sum_arr_prev > 0 then (sum_arr - sum_arr_prev) / sum_arr_prev else 1
    end as arr_change_pct,
    coalesce(sum_arr, 0) - coalesce(sum_arr_prev, 0) as sum_arr_change_amt,
    case
        when sum_mrr_prev > 0 then (sum_mrr - sum_mrr_prev) / sum_mrr_prev else 1
    end as mrr_change_pct,
    coalesce(sum_mrr, 0) - coalesce(sum_mrr_prev, 0) as mrr_change_amt,
    case
        when crm_account_tsp_account_employees_prev > 0
        then
            (crm_account_tsp_account_employees - crm_account_tsp_account_employees_prev)
            / crm_account_tsp_account_employees_prev
        else 1
    end as crm_account_tsp_account_employees_change_pct,
    coalesce(crm_account_tsp_account_employees, 0) - coalesce(
        crm_account_tsp_account_employees_prev, 0
    ) as crm_account_tsp_account_employees_change_cnt,
    case
        when num_of_subs_prev > 0
        then (num_of_subs - num_of_subs_prev) / num_of_subs_prev
        else 1
    end as subs_change_pct,
    coalesce(num_of_subs, 0) - coalesce(num_of_subs_prev, 0) as subs_change_cnt,
    case
        when license_count_prev > 0
        then (license_count - license_count_prev) / license_count_prev
        else 1
    end as license_change_pct,
    coalesce(license_count, 0) - coalesce(license_count_prev, 0) as license_change_cnt,
    case
        when cancelled_subs_prev > 0
        then (cancelled_subs - cancelled_subs_prev) / cancelled_subs_prev
        else 1
    end as cancelled_subs_change_pct,
    coalesce(cancelled_subs, 0)
    - coalesce(cancelled_subs_prev, 0) as cancelled_subs_change_cnt,
    coalesce(p1.self_managed_instance_count, 0) - coalesce(
        p2.self_managed_instance_count_prev, 0
    ) as self_managed_instance_change_cnt,
    coalesce(p1.saas_instance_count, 0)
    - coalesce(p2.saas_instance_count_prev, 0) as saas_instance_change_cnt,
    coalesce(p1.others_instance_count, 0)
    - coalesce(p2.others_instance_count_prev, 0) as others_instance_change_cnt,
    coalesce(p1.is_ultimate_product_tier, 0)
    - coalesce(p2.is_ultimate_product_tier_prev, 0) as ultimate_product_tier_change_cnt,
    coalesce(p1.is_premium_product_tier, 0)
    - coalesce(p2.is_premium_product_tier_prev, 0) as premium_product_tier_change_cnt,
    coalesce(p1.is_starter_bronze_product_tier, 0) - coalesce(
        p2.is_starter_bronze_product_tier_prev, 0
    ) as starter_bronze_product_tier_change_cnt,
    coalesce(p1.is_service_type_full_service, 0) - coalesce(
        p2.is_service_type_full_service_prev, 0
    ) as service_type_full_service_change_cnt,
    coalesce(p1.is_service_type_support_only, 0) - coalesce(
        p2.is_service_type_support_only_prev, 0
    ) as service_type_support_only_change_cnt,
    -- Salesforce Opportunity Fields
    coalesce(o.num_opportunities, 0) as opportunities_cnt,
    coalesce(o.sales_path_sales_assisted_cnt, 0) as sales_path_sales_assisted_cnt,
    coalesce(o.sales_path_web_direct_cnt, 0) as sales_path_web_direct_cnt,
    coalesce(o.deal_size_other_cnt, 0) as deal_size_other_cnt,
    coalesce(o.deal_size_small_cnt, 0) as deal_size_small_cnt,
    coalesce(o.deal_size_medium_cnt, 0) as deal_size_medium_cnt,
    coalesce(o.deal_size_big_cnt, 0) as deal_size_big_cnt,
    coalesce(o.deal_size_jumbo_cnt, 0) as deal_size_jumbo_cnt,
    coalesce(o.won_opportunities, 0) as won_opportunities_cnt,
    coalesce(o.lost_opportunities, 0) as lost_opportunities_cnt,
    coalesce(o.num_expansions, 0) as expansions_cnt,
    coalesce(o.num_contractions, 0) as contractions_cnt,
    coalesce(o.num_opportunities_by_renewal, 0) as opportunities_by_renewal_cnt,
    coalesce(o.num_opportunities_new_business, 0) as opportunities_new_business_cnt,
    coalesce(
        o.num_opportunities_add_on_business, 0
    ) as opportunities_add_on_business_cnt,
    coalesce(o.sum_net_arr, 0) as net_arr_amt,
    coalesce(o.sum_net_arr_won_opportunities, 0) as net_arr_won_opportunities_amt,
    coalesce(o.sum_net_arr_lost_opportunities, 0) as net_arr_lost_opportunities_amt,
    coalesce(o.won_opportunities_by_renewal, 0) as won_opportunities_by_renewal_cnt,
    coalesce(o.won_opportunities_new_business, 0) as won_opportunities_new_business_cnt,
    coalesce(
        o.won_opportunities_add_on_business, 0
    ) as won_opportunities_add_on_business_cnt,
    coalesce(o.lost_opportunities_by_renewal, 0) as lost_opportunities_by_renewal_cnt,
    coalesce(
        o.lost_opportunities_new_business, 0
    ) as lost_opportunities_new_business_cnt,
    coalesce(
        o.lost_opportunities_add_on_business, 0
    ) as lost_opportunities_add_on_business_cnt,
    coalesce(o.competitors_other, 0) as competitors_other_flag,
    coalesce(o.competitors_gitlab_core, 0) as competitors_gitlab_core_flag,
    coalesce(o.competitors_none, 0) as competitors_none_flag,
    coalesce(o.competitors_github_enterprise, 0) as competitors_github_enterprise_flag,
    coalesce(o.competitors_bitbucket_server, 0) as competitors_bitbucket_server_flag,
    coalesce(o.competitors_unknown, 0) as competitors_unknown_flag,
    coalesce(o.competitors_github, 0) as competitors_github_flag,
    coalesce(o.competitors_gitlab, 0) as competitors_gitlab_flag,
    coalesce(o.competitors_jenkins, 0) as competitors_jenkins_flag,
    coalesce(o.competitors_azure_devops, 0) as competitors_azure_devops_flag,
    coalesce(o.competitors_svn, 0) as competitors_svn_flag,
    coalesce(o.competitors_bitbucket, 0) as competitors_bitbucket_flag,
    coalesce(o.competitors_atlassian, 0) as competitors_atlassian_flag,
    coalesce(o.competitors_perforce, 0) as competitors_perforce_flag,
    coalesce(o.competitors_visual_studio, 0) as competitors_visual_studio_flag,
    coalesce(o.competitors_azure, 0) as competitors_azure_flag,
    coalesce(o.competitors_amazon_code_commit, 0) as competitors_amazon_code_commit_flag
    ,
    coalesce(o.competitors_circleci, 0) as competitors_circleci_flag,
    coalesce(o.competitors_bamboo, 0) as competitors_bamboo_flag,
    coalesce(o.competitors_aws, 0) as competitors_aws_flag,
    coalesce(
        o.use_case_continuous_integration, 0
    ) as use_case_continuous_integration_cnt,
    coalesce(o.use_case_dev_sec_ops, 0) as use_case_dev_sec_ops_cnt,
    coalesce(o.use_case_continuous_delivery, 0) as use_case_continuous_delivery_cnt,
    coalesce(
        o.use_case_version_controlled_configuration, 0
    ) as use_case_version_controlled_configuration_cnt,
    coalesce(o.use_case_simplify_dev_ops, 0) as use_case_simplify_dev_ops_cnt,
    coalesce(o.use_case_agile, 0) as use_case_agile_cnt,
    coalesce(o.use_case_other, 0) as use_case_other_cnt,
    coalesce(o.use_case_cloud_native, 0) as use_case_cloud_native_cnt,
    coalesce(o.use_case_git_ops, 0) as use_case_git_ops_cnt,
    case when o.account_id is not null then 1 else 0 end as has_sfdc_opportunities_flag,
    -- ZoomInfo Fields
    zt.zi_revenue as zi_revenue,
    zt.zi_industry as zi_industry,
    zt.zi_sic_code as zi_sic_code,
    zt.zi_naics_code as zi_naics_code,
    zt.zi_developers_cnt as zi_developers_cnt,
    coalesce(zt.zi_atlassian_flag, 0) as zi_atlassian_flag,
    coalesce(zt.zi_bitbucket_flag, 0) as zi_bitbucket_flag,
    coalesce(zt.zi_jira_flag, 0) as zi_jira_flag,
    coalesce(zt.zi_gcp_flag, 0) as zi_gcp_flag,
    coalesce(zt.zi_github_flag, 0) as zi_github_flag,
    coalesce(zt.zi_github_enterprise_flag, 0) as zi_github_enterprise_flag,
    coalesce(zt.zi_aws_flag, 0) as zi_aws_flag,
    coalesce(zt.zi_aws_iam_flag, 0) as zi_aws_iam_flag,
    coalesce(zt.zi_aws_cloud_trail_flag, 0) as zi_aws_cloud_trail_flag,
    coalesce(zt.zi_hashicorp_flag, 0) as zi_hashicorp_flag,
    coalesce(zt.zi_circleci_flag, 0) as zi_circleci_flag,
    coalesce(zt.zi_travisci_flag, 0) as zi_travisci_flag,
    coalesce(zt.zi_apache_subversion_flag, 0) as zi_apache_subversion_flag,
    coalesce(zt.zi_jenkins_flag, 0) as zi_jenkins_flag,
    coalesce(zt.zi_tortoise_svn_flag, 0) as zi_tortoise_svn_flag,
    coalesce(zt.zi_kubernetes_flag, 0) as zi_kubernetes_flag,
    coalesce(
        zt.zi_atlassian_flag, zt.zi_bitbucket_flag, zt.zi_jira_flag, 0
    ) as zi_atlassian_any_flag,
    coalesce(zt.zi_github_flag, zt.zi_github_enterprise_flag, 0) as zi_github_any_flag,
    coalesce(
        zt.zi_aws_flag, zt.zi_aws_iam_flag, zt.zi_aws_cloud_trail_flag, 0
    ) as zi_aws_any_flag,
    coalesce(
        zt.zi_hashicorp_flag, zt.zi_bitbucket_flag, zt.zi_jira_flag, 0
    ) as zi_other_ci_any_flag,
    coalesce(
        zt.zi_apache_subversion_flag,
        zt.zi_jenkins_flag,
        zt.zi_tortoise_svn_flag,
        zt.zi_kubernetes_flag,
        0
    ) as zi_open_source_any_flag,
    -- Event Salesforce
    coalesce(
        es.initial_qualifying_meeting_event_count, 0
    ) as initial_qualifying_meeting_event_cnt,
    coalesce(es.meeting_event_count, 0) as meeting_event_cnt,
    coalesce(es.web_conference_event_count, 0) as web_conference_event_cnt,
    coalesce(es.call_event_count, 0) as call_event_cnt,
    coalesce(es.demo_event_count, 0) as demo_event_cnt,
    coalesce(es.in_person_event_count, 0) as in_person_event_cnt,
    coalesce(es.renewal_event_count, 0) as renewal_event_cnt,
    coalesce(es.total_event_count, 0) as total_event_cnt,
    case when es.account_id is not null then 1 else 0 end as has_sfdc_events_flag,
    -- Task Salesforce
    coalesce(ts.email_task_count, 0) as email_task_cnt,
    coalesce(ts.call_task_count, 0) as call_task_cnt,
    coalesce(ts.demo_task_count, 0) as demo_task_cnt,
    coalesce(ts.sales_alignment_task_count, 0) as sales_alignment_task_cnt,
    coalesce(ts.total_task_count, 0) as total_task_cnt,
    coalesce(ts.is_answered_task, 0) as is_answered_task_flag,
    coalesce(ts.is_busy_task, 0) as is_busy_task_flag,
    coalesce(ts.is_correct_contact_task, 0) as is_correct_contact_task_flag,
    coalesce(ts.is_left_message_task, 0) as is_left_message_task_flag,
    coalesce(ts.is_not_answered_task, 0) as is_not_answered_task_flag,
    case when ts.account_id is not null then 1 else 0 end as has_sfdc_tasks_flag,
    -- Bizible Fields
    coalesce(b.num_bizible_touchpoints, 0) as bizible_touchpoints_cnt,
    coalesce(b.num_campaigns, 0) as campaigns_cnt,
    coalesce(b.touchpoint_source_web_direct, 0) as touchpoint_source_web_direct_cnt,
    coalesce(
        b.touchpoint_source_web_organic_google, 0
    ) as touchpoint_source_web_organic_google_cnt,
    coalesce(b.touchpoint_source_crm_campaign, 0) as touchpoint_source_crm_campaign_cnt,
    coalesce(b.touchpoint_source_marketo, 0) as touchpoint_source_marketo_cnt,
    coalesce(b.touchpoint_source_crm_activity, 0) as touchpoint_source_crm_activity_cnt,
    coalesce(b.touchpoint_source_social_media, 0) as touchpoint_source_social_media_cnt,
    coalesce(b.touchpoint_type_web_form, 0) as touchpoint_type_web_form_cnt,
    coalesce(b.touchpoint_type_web_visit, 0) as touchpoint_type_web_visit_cnt,
    coalesce(b.touchpoint_type_crm, 0) as touchpoint_type_crm_cnt,
    coalesce(b.touchpoint_type_iqm, 0) as touchpoint_type_iqm_cnt,
    coalesce(b.touchpoint_type_web_chat, 0) as touchpoint_type_web_chat_cnt,
    coalesce(
        b.touchpoint_marketing_channel_direct, 0
    ) as touchpoint_marketing_channel_direct_cnt,
    coalesce(
        b.touchpoint_marketing_channel_organic_search, 0
    ) as touchpoint_marketing_channel_organic_search_cnt,
    coalesce(
        b.touchpoint_marketing_channel_email, 0
    ) as touchpoint_marketing_channel_email_cnt,
    coalesce(
        b.touchpoint_marketing_channel_web_referral, 0
    ) as touchpoint_marketing_channel_web_referral_cnt,
    coalesce(
        b.touchpoint_marketing_channel_web_event, 0
    ) as touchpoint_marketing_channel_web_event_cnt,
    coalesce(
        b.touchpoint_type_inbound_request, 0
    ) as touchpoint_type_inbound_request_cnt,
    coalesce(b.touchpoint_type_direct_mail, 0) as touchpoint_type_direct_mail_cnt,
    coalesce(b.touchpoint_type_trial, 0) as touchpoint_type_trial_cnt,
    coalesce(b.touchpoint_type_webcast, 0) as touchpoint_type_webcast_cnt,
    coalesce(b.touchpoint_bizible_medium_web, 0) as touchpoint_bizible_medium_web_cnt,
    coalesce(
        b.touchpoint_bizible_medium_search, 0
    ) as touchpoint_bizible_medium_search_cnt,
    coalesce(
        b.touchpoint_bizible_medium_email, 0
    ) as touchpoint_bizible_medium_email_cnt,
    coalesce(
        b.touchpoint_bizible_medium_trial, 0
    ) as touchpoint_bizible_medium_trial_cnt,
    coalesce(
        b.touchpoint_bizible_medium_webcast, 0
    ) as touchpoint_bizible_medium_webcast_cnt,
    coalesce(
        b.touchpoint_crm_person_status_qualified, 0
    ) as touchpoint_crm_person_status_qualified_cnt,
    coalesce(
        b.touchpoint_crm_person_status_inquery, 0
    ) as touchpoint_crm_person_status_inquery_cnt,
    coalesce(
        b.touchpoint_crm_person_status_mql, 0
    ) as touchpoint_crm_person_status_mql_cnt,
    coalesce(
        b.touchpoint_crm_person_status_nurture, 0
    ) as touchpoint_crm_person_status_nurture_cnt,
    coalesce(
        b.touchpoint_crm_person_status_qualifying, 0
    ) as touchpoint_crm_person_status_qualifying_cnt,
    coalesce(
        b.touchpoint_crm_person_status_accepted, 0
    ) as touchpoint_crm_person_status_accepted_cnt,
    coalesce(
        b.touchpoint_crm_person_title_cto, 0
    ) as touchpoint_crm_person_title_cto_cnt,
    coalesce(
        b.touchpoint_crm_person_title_software_developer, 0
    ) as touchpoint_crm_person_title_software_developer_cnt,
    coalesce(
        b.touchpoint_crm_person_title_software_engineer, 0
    ) as touchpoint_crm_person_title_software_engineer_cnt,
    coalesce(
        b.touchpoint_crm_person_title_software_dev_team_lead, 0
    ) as touchpoint_crm_person_title_software_dev_team_lead_cnt,
    case
        when b.dim_crm_account_id is not null then 1 else 0
    end as has_bizible_data_flag,
    -- Product Usage
    u.unique_active_user as unique_active_user_cnt,
    u.action_monthly_active_users_project_repo_avg
    as action_monthly_active_users_project_repo_avg,
    u.merge_requests_avg as merge_requests_avg,
    u.projects_with_repositories_enabled_avg as projects_with_repositories_enabled_avg,
    u.ci_pipelines_avg as ci_pipelines_avg,
    u.ci_internal_pipelines_avg as ci_internal_pipelines_avg,
    u.ci_builds_avg as ci_builds_avg,
    u.ci_pipeline_config_repository_avg as ci_pipeline_config_repository_avg,
    u.user_unique_users_all_secure_scanners_avg
    as user_unique_users_all_secure_scanners_avg,
    u.user_sast_jobs_avg as user_sast_jobs_avg,
    u.user_dast_jobs_avg as user_dast_jobs_avg,
    u.user_dependency_scanning_jobs_avg as user_dependency_scanning_jobs_avg,
    u.user_license_management_jobs_avg as user_license_management_jobs_avg,
    u.user_secret_detection_jobs_avg as user_secret_detection_jobs_avg,
    u.user_container_scanning_jobs_avg as user_container_scanning_jobs_avg,
    u.deployments_avg as deployments_avg,
    u.releases_avg as releases_avg,
    u.epics_avg as epics_avg,
    u.issues_avg as issues_avg,
    u.analytics_avg as analytics_avg,
    u.commit_comment_all_time_event as commit_comment_all_time_event_cnt,
    u.source_code_pushes_all_time_event as source_code_pushes_all_time_event_cnt,
    u.template_repositories_all_time_event as template_repositories_all_time_event_cnt,
    u.ci_runners_all_time_event as ci_runners_all_time_event_cnt,
    u.auto_devops_enabled_all_time_event as auto_devops_enabled_all_time_event_cnt,
    u.projects_with_packages_all_time_event as projects_with_packages_all_time_event_cnt
    ,
    u.merge_requests_all_time_event as merge_requests_all_time_event_cnt,
    u.epics_all_time_event as epics_all_time_event_cnt,
    u.issues_all_time_event as issues_all_time_event_cnt,
    u.projects_all_time_event as projects_all_time_event_cnt,
    u.gitlab_shared_runners_enabled as gitlab_shared_runners_enabled_flag,
    u.container_registry_enabled as container_registry_enabled_flag,
    case
        when u.license_user_count_cnt > 0
        then u.active_user_count_cnt / u.license_user_count_cnt
        else 0
    end as license_utilization_pct,
    u.active_user_count_cnt as active_user_count_cnt,
    u.license_user_count_cnt as license_user_count_cnt,
    u.billable_user_count_cnt as billable_user_count_cnt,
    u.max_historical_user_count as max_historical_user_cnt,
    u.gitlab_version as gitlab_version,
    case when u.dim_crm_account_id is not null then 1 else 0 end as has_usage_data_flag

from period_1 p1
left join target t on p1.dim_crm_account_id = t.dim_crm_account_id
left join period_2 p2 on p1.dim_crm_account_id = p2.dim_crm_account_id
left join opps o on p1.dim_crm_account_id = o.account_id
left join events_salesforce es on p1.dim_crm_account_id = es.account_id
left join tasks_salesforce ts on p1.dim_crm_account_id = ts.account_id
left join zi_technologies zt on p1.dim_crm_account_id = zt.account_id
-- LEFT JOIN lifetime l
-- ON p1.dim_crm_account_id = l.dim_crm_account_id
left join bizible b on p1.dim_crm_account_id = b.dim_crm_account_id
left join product_usage u on p1.dim_crm_account_id = u.dim_crm_account_id

{%- endmacro -%}

{{ config(alias="sfdc_opportunity_xf") }}

-- +wk_sales_report_pipeline_metrics_per_day_with_targets
-- +wk_sales_report_pipeline_velocity_quarter_with_targets
-- +wk_sales_report_opportunity_pipeline_type
with
    sfdc_opportunity as (

        select opportunity_id, opportunity_category, product_category
        from {{ ref("sfdc_opportunity") }}

    ),
    sfdc_users_xf as (select * from {{ ref("wk_sales_sfdc_users_xf") }}),
    sfdc_accounts_xf as (select * from {{ ref("sfdc_accounts_xf") }}),
    date_details as (select * from {{ ref("wk_sales_date_details") }}),
    -- keys used for aggregated historical analysis
    agg_demo_keys as (select * from {{ ref("wk_sales_report_agg_demo_sqs_ot_keys") }}),
    today as (

        select distinct
            fiscal_year as current_fiscal_year,
            first_day_of_fiscal_year as current_fiscal_year_date
        from date_details
        where date_actual = current_date

    ),
    sfdc_opportunity_xf as (

        select
            sfdc_opportunity_xf.account_id,
            sfdc_opportunity_xf.opportunity_id,
            sfdc_opportunity_xf.opportunity_name,
            sfdc_opportunity_xf.owner_id,
            sfdc_opportunity_xf.close_date,
            sfdc_opportunity_xf.created_date,
            sfdc_opportunity_xf.days_in_stage,
            sfdc_opportunity_xf.deployment_preference,
            sfdc_opportunity_xf.generated_source,
            sfdc_opportunity_xf.lead_source,
            sfdc_opportunity_xf.lead_source_id,
            sfdc_opportunity_xf.lead_source_name,
            sfdc_opportunity_xf.lead_source_type,
            sfdc_opportunity_xf.merged_opportunity_id,
            sfdc_opportunity_xf.net_new_source_categories,
            sfdc_opportunity_xf.opportunity_business_development_representative,
            opportunity_owner.name as opportunity_owner,
            sfdc_opportunity_xf.opportunity_owner_department,
            sfdc_opportunity_xf.opportunity_owner_manager,
            sfdc_opportunity_xf.opportunity_owner_role,
            sfdc_opportunity_xf.opportunity_owner_title,
            sfdc_opportunity_xf.opportunity_sales_development_representative,
            sfdc_opportunity_xf.opportunity_development_representative,
            sfdc_opportunity_xf.opportunity_term,
            sfdc_opportunity_xf.primary_campaign_source_id,
            sfdc_opportunity_xf.sales_accepted_date,
            sfdc_opportunity_xf.sales_path,
            sfdc_opportunity_xf.sales_qualified_date,
            sfdc_opportunity_xf.sales_type,
            sfdc_opportunity_xf.sdr_pipeline_contribution,
            sfdc_opportunity_xf.source_buckets,
            sfdc_opportunity_xf.stage_name,
            sfdc_opportunity_xf.stage_is_active,
            sfdc_opportunity_xf.stage_is_closed,
            sfdc_opportunity_xf.technical_evaluation_date,

            sfdc_opportunity_xf.acv,
            sfdc_opportunity_xf.amount,
            sfdc_opportunity_xf.closed_deals,
            sfdc_opportunity_xf.competitors,
            sfdc_opportunity_xf.critical_deal_flag,
            sfdc_opportunity_xf.fpa_master_bookings_flag,

            -- Deal Size field is wrong in the source object
            -- it is using
            -- sfdc_opportunity_xf.deal_size,    
            sfdc_opportunity_xf.forecast_category_name,
            sfdc_opportunity_xf.forecasted_iacv,
            sfdc_opportunity_xf.incremental_acv,
            sfdc_opportunity_xf.invoice_number,

            -- logic needs to be added here once the oppotunity category fields is
            -- merged
            -- https://gitlab.com/gitlab-data/analytics/-/issues/7888
            -- sfdc_opportunity_xf.is_refund,
            case
                when sfdc_opportunity.opportunity_category in ('Decommission')
                then 1
                else 0
            end as is_refund,

            case
                when sfdc_opportunity.opportunity_category in ('Credit') then 1 else 0
            end as is_credit_flag,

            case
                when sfdc_opportunity.opportunity_category in ('Contract Reset')
                then 1
                else 0
            end as is_contract_reset_flag,

            sfdc_opportunity_xf.is_downgrade,
            sfdc_opportunity_xf.is_edu_oss,
            cast(sfdc_opportunity_xf.is_won as integer) as is_won,
            sfdc_opportunity_xf.net_incremental_acv,
            sfdc_opportunity_xf.professional_services_value,
            sfdc_opportunity_xf.reason_for_loss,
            sfdc_opportunity_xf.reason_for_loss_details,
            sfdc_opportunity_xf.downgrade_reason,
            sfdc_opportunity_xf.renewal_acv,
            sfdc_opportunity_xf.renewal_amount,
            case
                when sfdc_opportunity_xf.sales_qualified_source = 'BDR Generated'
                then 'SDR Generated'
                else coalesce(sfdc_opportunity_xf.sales_qualified_source, 'NA')
            end as sales_qualified_source,

            sfdc_opportunity_xf.solutions_to_be_replaced,
            sfdc_opportunity_xf.total_contract_value,
            sfdc_opportunity_xf.upside_iacv,
            sfdc_opportunity_xf.is_web_portal_purchase,
            sfdc_opportunity_xf.subscription_start_date,
            sfdc_opportunity_xf.subscription_end_date,

            -- ---------------------------------------------------------
            -- ---------------------------------------------------------
            -- New fields for FY22 - including user segment / region fields
            sfdc_opportunity_xf.order_type_live,
            sfdc_opportunity_xf.order_type_stamped,

            coalesce(sfdc_opportunity_xf.net_arr, 0) as raw_net_arr,
            sfdc_opportunity_xf.recurring_amount,
            sfdc_opportunity_xf.true_up_amount,
            sfdc_opportunity_xf.proserv_amount,
            sfdc_opportunity_xf.other_non_recurring_amount,
            sfdc_opportunity_xf.arr_basis,
            sfdc_opportunity_xf.arr,

            -- ---------------------------------------------------------
            -- ---------------------------------------------------------
            sfdc_opportunity_xf.opportunity_health,
            sfdc_opportunity_xf.is_risky,
            sfdc_opportunity_xf.risk_type,
            sfdc_opportunity_xf.risk_reasons,
            sfdc_opportunity_xf.tam_notes,
            sfdc_opportunity_xf.days_in_1_discovery,
            sfdc_opportunity_xf.days_in_2_scoping,
            sfdc_opportunity_xf.days_in_3_technical_evaluation,
            sfdc_opportunity_xf.days_in_4_proposal,
            sfdc_opportunity_xf.days_in_5_negotiating,
            sfdc_opportunity_xf.stage_0_pending_acceptance_date,
            sfdc_opportunity_xf.stage_1_discovery_date,
            sfdc_opportunity_xf.stage_2_scoping_date,
            sfdc_opportunity_xf.stage_3_technical_evaluation_date,
            sfdc_opportunity_xf.stage_4_proposal_date,
            sfdc_opportunity_xf.stage_5_negotiating_date,
            sfdc_opportunity_xf.stage_6_awaiting_signature_date,
            sfdc_opportunity_xf.stage_6_closed_won_date,
            sfdc_opportunity_xf.stage_6_closed_lost_date,
            sfdc_opportunity_xf.cp_champion,
            sfdc_opportunity_xf.cp_close_plan,
            sfdc_opportunity_xf.cp_competition,
            sfdc_opportunity_xf.cp_decision_criteria,
            sfdc_opportunity_xf.cp_decision_process,
            sfdc_opportunity_xf.cp_economic_buyer,
            sfdc_opportunity_xf.cp_identify_pain,
            sfdc_opportunity_xf.cp_metrics,
            sfdc_opportunity_xf.cp_risks,
            sfdc_opportunity_xf.cp_use_cases,
            sfdc_opportunity_xf.cp_value_driver,
            sfdc_opportunity_xf.cp_why_do_anything_at_all,
            sfdc_opportunity_xf.cp_why_gitlab,
            sfdc_opportunity_xf.cp_why_now,

            -- ---------------------------------------------------------
            -- ---------------------------------------------------------
            -- used for segment reporting in FY21 and before
            sfdc_opportunity_xf.account_owner_team_stamped,

            -- NF: why do we need these fields now?
            sfdc_opportunity_xf.division_sales_segment_stamped,
            sfdc_opportunity_xf.tsp_max_hierarchy_sales_segment,
            sfdc_opportunity_xf.division_sales_segment,
            sfdc_opportunity_xf.ultimate_parent_sales_segment,
            sfdc_opportunity_xf.sales_segment,
            sfdc_opportunity_xf.parent_segment,

            -- ---------------------------------------------------------
            -- ---------------------------------------------------------
            -- Channel Org. fields
            sfdc_opportunity_xf.deal_path,
            sfdc_opportunity_xf.dr_partner_deal_type,
            sfdc_opportunity_xf.dr_partner_engagement,
            sfdc_opportunity_xf.partner_account as partner_account,
            partner_account.account_name as partner_account_name,
            sfdc_opportunity_xf.dr_status,
            sfdc_opportunity_xf.distributor,

            sfdc_opportunity_xf.influence_partner,

            -- --------------------------------------------------------
            -- NF 20211108 this field should be removed when possible, need to
            -- validate with Channel Ops
            sfdc_opportunity_xf.fulfillment_partner,
            -- --------------------------------------------------------
            sfdc_opportunity_xf.fulfillment_partner as resale_partner_id,
            resale_account.account_name as resale_partner_name,
            sfdc_opportunity_xf.platform_partner,

            case
                when sfdc_opportunity_xf.deal_path = 'Channel'
                then
                    replace(
                        coalesce(
                            sfdc_opportunity_xf.partner_track,
                            partner_account.partner_track,
                            resale_account.partner_track,
                            'Open'
                        ),
                        'select',
                        'Select'
                    )
                else 'Direct'
            end as calculated_partner_track,


            sfdc_opportunity_xf.partner_track as partner_track,
            partner_account.gitlab_partner_program as partner_gitlab_program,

            sfdc_opportunity_xf.is_public_sector_opp,
            sfdc_opportunity_xf.is_registration_from_portal,
            sfdc_opportunity_xf.calculated_discount,
            sfdc_opportunity_xf.partner_discount,
            sfdc_opportunity_xf.partner_discount_calc,
            sfdc_opportunity_xf.comp_channel_neutral,

            case
                when sfdc_opportunity_xf.deal_path = 'Direct'
                then 'Direct'
                when sfdc_opportunity_xf.deal_path = 'Web Direct'
                then 'Web Direct'
                when
                    sfdc_opportunity_xf.deal_path = 'Channel'
                    and sfdc_opportunity_xf.sales_qualified_source = 'Channel Generated'
                then 'Partner Sourced'
                when
                    sfdc_opportunity_xf.deal_path = 'Channel'
                    and sfdc_opportunity_xf.sales_qualified_source
                    != 'Channel Generated'
                then 'Partner Co-Sell'
            end as deal_path_engagement,


            sfdc_opportunity_xf.stage_name_3plus,
            sfdc_opportunity_xf.stage_name_4plus,
            sfdc_opportunity_xf.is_stage_3_plus,
            sfdc_opportunity_xf.is_lost,

            -- NF: Excluded 'Duplicate' stage from is_open definition
            case
                when
                    sfdc_opportunity_xf.stage_name
                    in ('8-Closed Lost', '9-Unqualified', 'Closed Won', '10-Duplicate')
                then 0
                else 1
            end as is_open,

            case
                when sfdc_opportunity_xf.stage_name in ('10-Duplicate') then 1 else 0
            end as is_duplicate_flag,

            sfdc_opportunity_xf.is_closed,
            sfdc_opportunity_xf.stage_category,
            sfdc_opportunity_xf.is_renewal,
            sfdc_opportunity_xf.close_fiscal_quarter_name,
            sfdc_opportunity_xf.close_fiscal_quarter_date,
            sfdc_opportunity_xf.close_fiscal_year,
            sfdc_opportunity_xf.close_date_month,
            sfdc_opportunity_xf.created_fiscal_quarter_name,
            sfdc_opportunity_xf.created_fiscal_quarter_date,
            sfdc_opportunity_xf.created_fiscal_year,
            sfdc_opportunity_xf.created_date_month,
            sfdc_opportunity_xf.subscription_start_date_fiscal_quarter_name,
            sfdc_opportunity_xf.subscription_start_date_fiscal_quarter_date,
            sfdc_opportunity_xf.subscription_start_date_fiscal_year,
            sfdc_opportunity_xf.subscription_start_date_month,
            sfdc_opportunity_xf.sales_accepted_fiscal_quarter_name,
            sfdc_opportunity_xf.sales_accepted_fiscal_quarter_date,
            sfdc_opportunity_xf.sales_accepted_fiscal_year,
            sfdc_opportunity_xf.sales_accepted_date_month as sales_accepted_month,
            sfdc_opportunity_xf.sales_qualified_fiscal_quarter_name,
            sfdc_opportunity_xf.sales_qualified_fiscal_quarter_date,
            sfdc_opportunity_xf.sales_qualified_fiscal_year,
            sfdc_opportunity_xf.sales_qualified_date_month,

            -- Net ARR Created Date uses the same old IACV Created date field in SFDC
            -- As long as the field in the legacy model is not renamed, this will work
            sfdc_opportunity_xf.iacv_created_date as net_arr_created_date,
            sfdc_opportunity_xf.iacv_created_fiscal_quarter_name
            as net_arr_created_fiscal_quarter_name,
            sfdc_opportunity_xf.iacv_created_fiscal_quarter_date
            as net_arr_created_fiscal_quarter_date,
            sfdc_opportunity_xf.iacv_created_fiscal_year as net_arr_created_fiscal_year,
            sfdc_opportunity_xf.iacv_created_date_month as net_arr_created_date_month,

            stage_1_date.date_actual as stage_1_date,
            stage_1_date.first_day_of_month as stage_1_date_month,
            stage_1_date.fiscal_year as stage_1_fiscal_year,
            stage_1_date.fiscal_quarter_name_fy as stage_1_fiscal_quarter_name,
            stage_1_date.first_day_of_fiscal_quarter as stage_1_fiscal_quarter_date,

            stage_3_date.date_actual as stage_3_date,
            stage_3_date.first_day_of_month as stage_3_date_month,
            stage_3_date.fiscal_year as stage_3_fiscal_year,
            stage_3_date.fiscal_quarter_name_fy as stage_3_fiscal_quarter_name,
            stage_3_date.first_day_of_fiscal_quarter as stage_3_fiscal_quarter_date,

            -- ----------------------------------
            sfdc_opportunity_xf._last_dbt_run,
            sfdc_opportunity_xf.business_process_id,
            sfdc_opportunity_xf.days_since_last_activity,
            sfdc_opportunity_xf.is_deleted,
            sfdc_opportunity_xf.last_activity_date,
            sfdc_opportunity_xf.record_type_description,
            sfdc_opportunity_xf.record_type_id,
            sfdc_opportunity_xf.record_type_label,
            sfdc_opportunity_xf.record_type_modifying_object_type,
            sfdc_opportunity_xf.record_type_name,
            sfdc_opportunity_xf.region_quota_id,
            sfdc_opportunity_xf.sales_quota_id,

            -- ---------------------------------------------------------------------------------------------------
            -- ---------------------------------------------------------------------------------------------------
            -- Opportunity User fields
            -- https://gitlab.my.salesforce.com/00N6100000ICcrD?setupid=OpportunityFields
            -- Team Segment / ASM - RD 
            -- NF 2022-01-28 Data seems clean in SFDC, but leving the fallback just in
            -- case
            case
                when sfdc_opportunity_xf.user_segment_stamped is null
                then opportunity_owner.user_segment
                else sfdc_opportunity_xf.user_segment_stamped
            end as opportunity_owner_user_segment,

            case
                when sfdc_opportunity_xf.user_geo_stamped is null
                then opportunity_owner.user_geo
                else sfdc_opportunity_xf.user_geo_stamped
            end as opportunity_owner_user_geo,

            case
                when sfdc_opportunity_xf.user_region_stamped is null
                then opportunity_owner.user_region
                else sfdc_opportunity_xf.user_region_stamped
            end as opportunity_owner_user_region,

            case
                when sfdc_opportunity_xf.user_area_stamped is null
                then opportunity_owner.user_area
                else sfdc_opportunity_xf.user_area_stamped
            end as opportunity_owner_user_area,
            -- opportunity_owner_subarea_stamped
            -- NF: 20210827 Fields for competitor analysis 
            case
                when contains(sfdc_opportunity_xf.competitors, 'Other') then 1 else 0
            end as competitors_other_flag,
            case
                when contains(sfdc_opportunity_xf.competitors, 'GitLab Core')
                then 1
                else 0
            end as competitors_gitlab_core_flag,
            case
                when contains(sfdc_opportunity_xf.competitors, 'None') then 1 else 0
            end as competitors_none_flag,
            case
                when contains(sfdc_opportunity_xf.competitors, 'GitHub Enterprise')
                then 1
                else 0
            end as competitors_github_enterprise_flag,
            case
                when contains(sfdc_opportunity_xf.competitors, 'BitBucket Server')
                then 1
                else 0
            end as competitors_bitbucket_server_flag,
            case
                when contains(sfdc_opportunity_xf.competitors, 'Unknown') then 1 else 0
            end as competitors_unknown_flag,
            case
                when contains(sfdc_opportunity_xf.competitors, 'GitHub.com')
                then 1
                else 0
            end as competitors_github_flag,
            case
                when contains(sfdc_opportunity_xf.competitors, 'GitLab.com')
                then 1
                else 0
            end as competitors_gitlab_flag,
            case
                when contains(sfdc_opportunity_xf.competitors, 'Jenkins') then 1 else 0
            end as competitors_jenkins_flag,
            case
                when contains(sfdc_opportunity_xf.competitors, 'Azure DevOps')
                then 1
                else 0
            end as competitors_azure_devops_flag,
            case
                when contains(sfdc_opportunity_xf.competitors, 'SVN') then 1 else 0
            end as competitors_svn_flag,
            case
                when contains(sfdc_opportunity_xf.competitors, 'BitBucket.Org')
                then 1
                else 0
            end as competitors_bitbucket_flag,
            case
                when contains(sfdc_opportunity_xf.competitors, 'Atlassian')
                then 1
                else 0
            end as competitors_atlassian_flag,
            case
                when contains(sfdc_opportunity_xf.competitors, 'Perforce') then 1 else 0
            end as competitors_perforce_flag,
            case
                when
                    contains(
                        sfdc_opportunity_xf.competitors, 'Visual Studio Team Services'
                    )
                then 1
                else 0
            end as competitors_visual_studio_flag,
            case
                when contains(sfdc_opportunity_xf.competitors, 'Azure') then 1 else 0
            end as competitors_azure_flag,
            case
                when contains(sfdc_opportunity_xf.competitors, 'Amazon Code Commit')
                then 1
                else 0
            end as competitors_amazon_code_commit_flag,
            case
                when contains(sfdc_opportunity_xf.competitors, 'CircleCI') then 1 else 0
            end as competitors_circleci_flag,
            case
                when contains(sfdc_opportunity_xf.competitors, 'Bamboo') then 1 else 0
            end as competitors_bamboo_flag,
            case
                when contains(sfdc_opportunity_xf.competitors, 'AWS') then 1 else 0
            end as competitors_aws_flag,


            -- ---------------------------------------------------------------------------------------------------
            -- ---------------------------------------------------------------------------------------------------
            -- sfdc_opportunity_xf.partner_initiated_opportunity,
            -- sfdc_opportunity_xf.true_up_value,
            -- sfdc_opportunity_xf.is_swing_deal,
            -- sfdc_opportunity_xf.probability,
            -- sfdc_opportunity_xf.pushed_count,
            -- sfdc_opportunity_xf.refund_iacv,
            -- sfdc_opportunity_xf.downgrade_iacv,
            -- sfdc_opportunity_xf.upside_swing_deal_iacv,
            -- sfdc_opportunity_xf.weighted_iacv,
            -- fields form opportunity source
            sfdc_opportunity.opportunity_category,
            sfdc_opportunity.product_category

        from {{ ref("sfdc_opportunity_xf") }} sfdc_opportunity_xf
        -- not all fields are in opportunity xf
        inner join
            sfdc_opportunity
            on sfdc_opportunity.opportunity_id = sfdc_opportunity_xf.opportunity_id
        inner join
            sfdc_users_xf opportunity_owner
            on opportunity_owner.user_id = sfdc_opportunity_xf.owner_id
        -- pipeline creation date
        left join
            date_details stage_1_date
            on stage_1_date.date_actual
            = sfdc_opportunity_xf.stage_1_discovery_date::date
        -- pipeline creation date
        left join
            date_details stage_3_date
            on stage_3_date.date_actual
            = sfdc_opportunity_xf.stage_3_technical_evaluation_date::date
        -- partner account details
        left join
            sfdc_accounts_xf partner_account
            on partner_account.account_id = sfdc_opportunity_xf.partner_account
        -- NF 20211105 resale partner
        left join
            sfdc_accounts_xf resale_account
            on resale_account.account_id = sfdc_opportunity_xf.fulfillment_partner
        -- NF 20210906 remove JiHu opties from the models
        where sfdc_opportunity_xf.is_jihu_account = 0

    ),
    net_iacv_to_net_arr_ratio as (

        select
            '2. New - Connected' as "ORDER_TYPE_STAMPED",
            'Mid-Market' as "USER_SEGMENT_STAMPED",
            0.999691784 as "RATIO_NET_IACV_TO_NET_ARR"
        union
        select
            '1. New - First Order' as "ORDER_TYPE_STAMPED",
            'SMB' as "USER_SEGMENT_STAMPED",
            0.998590143 as "RATIO_NET_IACV_TO_NET_ARR"
        union
        select
            '1. New - First Order' as "ORDER_TYPE_STAMPED",
            'Large' as "USER_SEGMENT_STAMPED",
            0.992289340 as "RATIO_NET_IACV_TO_NET_ARR"
        union
        select
            '3. Growth' as "ORDER_TYPE_STAMPED",
            'SMB' as "USER_SEGMENT_STAMPED",
            0.927846192 as "RATIO_NET_IACV_TO_NET_ARR"
        union
        select
            '3. Growth' as "ORDER_TYPE_STAMPED",
            'Large' as "USER_SEGMENT_STAMPED",
            0.852915435 as "RATIO_NET_IACV_TO_NET_ARR"
        union
        select
            '2. New - Connected' as "ORDER_TYPE_STAMPED",
            'SMB' as "USER_SEGMENT_STAMPED",
            1.009262672 as "RATIO_NET_IACV_TO_NET_ARR"
        union
        select
            '3. Growth' as "ORDER_TYPE_STAMPED",
            'Mid-Market' as "USER_SEGMENT_STAMPED",
            0.793618079 as "RATIO_NET_IACV_TO_NET_ARR"
        union
        select
            '1. New - First Order' as "ORDER_TYPE_STAMPED",
            'Mid-Market' as "USER_SEGMENT_STAMPED",
            0.988527875 as "RATIO_NET_IACV_TO_NET_ARR"
        union
        select
            '2. New - Connected' as "ORDER_TYPE_STAMPED",
            'Large' as "USER_SEGMENT_STAMPED",
            1.010081083 as "RATIO_NET_IACV_TO_NET_ARR"
        union
        select
            '1. New - First Order' as "ORDER_TYPE_STAMPED",
            'PubSec' as "USER_SEGMENT_STAMPED",
            1.000000000 as "RATIO_NET_IACV_TO_NET_ARR"
        union
        select
            '2. New - Connected' as "ORDER_TYPE_STAMPED",
            'PubSec' as "USER_SEGMENT_STAMPED",
            1.002741689 as "RATIO_NET_IACV_TO_NET_ARR"
        union
        select
            '3. Growth' as "ORDER_TYPE_STAMPED",
            'PubSec' as "USER_SEGMENT_STAMPED",
            0.965670500 as "RATIO_NET_IACV_TO_NET_ARR"


    ),
    churn_metrics as (

        select
            o.opportunity_id,
            nvl(o.reason_for_loss, o.downgrade_reason) as reason_for_loss_staged,
            case
                when
                    reason_for_loss_staged
                    in ('Do Nothing', 'Other', 'Competitive Loss', 'Operational Silos')
                    or reason_for_loss_staged is null
                then 'Unknown'
                when
                    reason_for_loss_staged in (
                        'Missing Feature',
                        'Product value/gaps',
                        'Product Value / Gaps',
                        'Stayed with Community Edition',
                        'Budget/Value Unperceived'
                    )
                then 'Product Value / Gaps'
                when
                    reason_for_loss_staged in (
                        'Lack of Engagement / Sponsor', 'Went Silent', 'Evangelist Left'
                    )
                then 'Lack of Engagement / Sponsor'
                when reason_for_loss_staged in ('Loss of Budget', 'No budget')
                then 'Loss of Budget'
                when reason_for_loss_staged = 'Merged into another opportunity'
                then 'Merged Opp'
                when reason_for_loss_staged = 'Stale Opportunity'
                then 'No Progression - Auto-close'
                when
                    reason_for_loss_staged in (
                        'Product Quality / Availability', 'Product quality/availability'
                    )
                then 'Product Quality / Availability'
                else reason_for_loss_staged
            end as reason_for_loss_calc,
            o.reason_for_loss_details,

            case
                when o.order_type_stamped in ('4. Contraction', '5. Churn - Partial')
                then 'Contraction'
                else 'Churn'
            end as churn_contraction_type_calc

        from sfdc_opportunity_xf o
        where
            o.order_type_stamped
            in ('4. Contraction', '5. Churn - Partial', '6. Churn - Final')
            and (o.is_won = 1 or (is_renewal = 1 and is_lost = 1))

    ),
    oppty_final as (

        select

            sfdc_opportunity_xf.*,

            -- date helpers
            -- pipeline created, tracks the date pipeline value was created for the
            -- first time
            -- used for performance reporting on pipeline generation
            -- these fields might change, isolating the field from the purpose
            -- alternatives are a future net_arr_created_date
            sfdc_opportunity_xf.net_arr_created_date as pipeline_created_date,
            sfdc_opportunity_xf.net_arr_created_date_month
            as pipeline_created_date_month,
            sfdc_opportunity_xf.net_arr_created_fiscal_year
            as pipeline_created_fiscal_year,
            sfdc_opportunity_xf.net_arr_created_fiscal_quarter_name
            as pipeline_created_fiscal_quarter_name,
            sfdc_opportunity_xf.net_arr_created_fiscal_quarter_date
            as pipeline_created_fiscal_quarter_date,

            /*
      FY23 fields
      2022-01-28 NF

        There are different layers of reporting.
        Account Owner -> Used to report performance of territories year over year, they are comparable across years 
          as it will be restated for all accounts after carving
        Opportunity Owner -> Used to report performance, the team might be different to the Account Owner due to holdovers 
          (accounts kept by a Sales Rep for a certain amount of time)
        Account Demographics -> The fields that would be appropiate to that account according to their address, it might not match the one
          of the account owner
        Report -> This will be a calculated field, using Opportunity Owner for current fiscal year opties and Account for anything before
        Sales Team -> Same as report, but with a naming convention closer to the sales org hierarchy

      */
            case
                when sfdc_opportunity_xf.close_date < today.current_fiscal_year_date
                then sfdc_accounts_xf.account_owner_user_segment
                else sfdc_opportunity_xf.opportunity_owner_user_segment
            end as report_opportunity_user_segment,

            case
                when sfdc_opportunity_xf.close_date < today.current_fiscal_year_date
                then sfdc_accounts_xf.account_owner_user_geo
                else sfdc_opportunity_xf.opportunity_owner_user_geo
            end as report_opportunity_user_geo,

            case
                when sfdc_opportunity_xf.close_date < today.current_fiscal_year_date
                then sfdc_accounts_xf.account_owner_user_region
                else sfdc_opportunity_xf.opportunity_owner_user_region
            end as report_opportunity_user_region,

            case
                when sfdc_opportunity_xf.close_date < today.current_fiscal_year_date
                then sfdc_accounts_xf.account_owner_user_area
                else sfdc_opportunity_xf.opportunity_owner_user_area
            end as report_opportunity_user_area,
            -- report_opportunity_subarea
            -- -----------------
            -- BASE KEYS
            -- 20220214 NF: Temporary keys, until the SFDC key is exposed
            lower(
                concat(
                    sfdc_opportunity_xf.opportunity_owner_user_segment,
                    '-',
                    sfdc_opportunity_xf.opportunity_owner_user_geo,
                    '-',
                    sfdc_opportunity_xf.opportunity_owner_user_region,
                    '-',
                    sfdc_opportunity_xf.opportunity_owner_user_area
                )
            ) as opportunity_user_segment_geo_region_area,

            -- NF 2022-02-17 these next two fields leverage the logic of comparing
            -- current fy opportunity demographics stamped vs account demo for
            -- previous years
            lower(
                concat(
                    report_opportunity_user_segment,
                    '-',
                    report_opportunity_user_geo,
                    '-',
                    report_opportunity_user_region,
                    '-',
                    report_opportunity_user_area
                )
            ) as report_user_segment_geo_region_area,
            lower(
                concat(
                    report_opportunity_user_segment,
                    '-',
                    report_opportunity_user_geo,
                    '-',
                    report_opportunity_user_region,
                    '-',
                    report_opportunity_user_area,
                    '-',
                    sfdc_opportunity_xf.sales_qualified_source,
                    '-',
                    sfdc_opportunity_xf.order_type_stamped
                )
            ) as report_user_segment_geo_region_area_sqs_ot,

            -- account driven fields 
            sfdc_accounts_xf.account_name,
            sfdc_accounts_xf.ultimate_parent_account_id,
            sfdc_accounts_xf.is_jihu_account,

            sfdc_accounts_xf.account_owner_user_segment,
            sfdc_accounts_xf.account_owner_user_geo,
            sfdc_accounts_xf.account_owner_user_region,
            sfdc_accounts_xf.account_owner_user_area,
            -- account_owner_subarea_stamped
            sfdc_accounts_xf.account_demographics_sales_segment
            as account_demographics_segment,
            sfdc_accounts_xf.account_demographics_geo,
            sfdc_accounts_xf.account_demographics_region,
            sfdc_accounts_xf.account_demographics_area,
            sfdc_accounts_xf.account_demographics_territory,
            -- account_demographics_subarea_stamped
            sfdc_accounts_xf.account_demographics_sales_segment
            as upa_demographics_segment,
            sfdc_accounts_xf.account_demographics_geo as upa_demographics_geo,
            sfdc_accounts_xf.account_demographics_region as upa_demographics_region,
            sfdc_accounts_xf.account_demographics_area as upa_demographics_area,
            sfdc_accounts_xf.account_demographics_territory
            as upa_demographics_territory,
            -- ---------------------------------------------
            case
                when
                    sfdc_opportunity_xf.stage_name in (
                        '1-Discovery',
                        '2-Developing',
                        '2-Scoping',
                        '3-Technical Evaluation',
                        '4-Proposal',
                        'Closed Won',
                        '5-Negotiating',
                        '6-Awaiting Signature',
                        '7-Closing'
                    )
                then 1
                else 0
            end as is_stage_1_plus,

            case
                when
                    sfdc_opportunity_xf.stage_name in (
                        '4-Proposal',
                        'Closed Won',
                        '5-Negotiating',
                        '6-Awaiting Signature',
                        '7-Closing'
                    )
                then 1
                else 0
            end as is_stage_4_plus,

            -- medium level grouping of the order type field
            case
                when sfdc_opportunity_xf.order_type_stamped = '1. New - First Order'
                then '1. New'
                when
                    sfdc_opportunity_xf.order_type_stamped
                    in ('2. New - Connected', '3. Growth')
                then '2. Growth'
                when sfdc_opportunity_xf.order_type_stamped in ('4. Contraction')
                then '3. Contraction'
                when
                    sfdc_opportunity_xf.order_type_stamped
                    in ('5. Churn - Partial', '6. Churn - Final')
                then '4. Churn'
                else '5. Other'
            end as deal_category,

            case
                when sfdc_opportunity_xf.order_type_stamped = '1. New - First Order'
                then '1. New'
                when
                    sfdc_opportunity_xf.order_type_stamped in (
                        '2. New - Connected',
                        '3. Growth',
                        '5. Churn - Partial',
                        '6. Churn - Final',
                        '4. Contraction'
                    )
                then '2. Growth'
                else '3. Other'
            end as deal_group,

            -- --------------------------------------------------------------
            -- --------------------------------------------------------------
            -- Temporary, to deal with global Bookings FY21 reports that use
            -- account_owner_team_stamp field
            -- NF 2022-01-28 TO BE REMOVED
            case
                when
                    sfdc_opportunity_xf.account_owner_team_stamped
                    in ('Commercial - SMB', 'SMB', 'SMB - US', 'SMB - International')
                then 'SMB'
                when
                    sfdc_opportunity_xf.account_owner_team_stamped in (
                        'APAC', 'EMEA', 'Channel', 'US West', 'US East', 'Public Sector'
                    )
                then 'Large'
                when
                    sfdc_opportunity_xf.account_owner_team_stamped in (
                        'MM - APAC',
                        'MM - East',
                        'MM - EMEA',
                        'Commercial - MM',
                        'MM - West',
                        'MM-EMEA'
                    )
                then 'Mid-Market'
                else 'SMB'
            end as account_owner_team_stamped_cro_level,

            -- --------------------------------------------------------------
            -- --------------------------------------------------------------
            -- fields for counting new logos, these fields count refund as negative
            case
                when sfdc_opportunity_xf.is_refund = 1
                then -1
                when sfdc_opportunity_xf.is_credit_flag = 1
                then 0
                else 1
            end as calculated_deal_count,

            -- --------------------------------------------------------------
            -- NF 2022-01-28 This is probably TO BE DEPRECATED too, need to align with
            -- Channel ops
            -- PIO Flag for PIO reporting dashboard
            case
                when sfdc_opportunity_xf.dr_partner_engagement = 'PIO' then 1 else 0
            end as partner_engaged_opportunity_flag,

            -- check if renewal was closed on time or not
            case
                when
                    sfdc_opportunity_xf.is_renewal = 1
                    and sfdc_opportunity_xf.subscription_start_date_fiscal_quarter_date
                    >= sfdc_opportunity_xf.close_fiscal_quarter_date
                then 'On-Time'
                when
                    sfdc_opportunity_xf.is_renewal = 1
                    and sfdc_opportunity_xf.subscription_start_date_fiscal_quarter_date
                    < sfdc_opportunity_xf.close_fiscal_quarter_date
                then 'Late'
            end as renewal_timing_status,

            -- --------------------------------------------------------------
            -- --------------------------------------------------------------
            -- calculated fields for pipeline velocity report
            -- 20201021 NF: This should be replaced by a table that keeps track of
            -- excluded deals for forecasting purposes
            case
                when
                    sfdc_accounts_xf.ultimate_parent_id in (
                        '001610000111bA3',
                        '0016100001F4xla',
                        '0016100001CXGCs',
                        '00161000015O9Yn',
                        '0016100001b9Jsc'
                    )
                    and sfdc_opportunity_xf.close_date < '2020-08-01'
                then 1
                -- NF 2021 - Pubsec extreme deals
                when
                    sfdc_opportunity_xf.opportunity_id
                    in ('0064M00000WtZKUQA3', '0064M00000Xb975QAB')
                then 1
                -- NF 20220415 PubSec duplicated deals on Pipe Gen -- Lockheed Martin
                -- GV - 40000 Ultimate Renewal
                when
                    sfdc_opportunity_xf.opportunity_id in (
                        '0064M00000ZGpfQQAT', '0064M00000ZGpfVQAT', '0064M00000ZGpfGQAT'
                    )
                then 1
                else 0
            end as is_excluded_flag,

            -- Customer Success related fields
            -- DRI Michael Armtz
            churn_metrics.reason_for_loss_staged,
            churn_metrics.reason_for_loss_calc,
            churn_metrics.churn_contraction_type_calc

        from sfdc_opportunity_xf
        cross join today
        left join
            sfdc_accounts_xf
            on sfdc_accounts_xf.account_id = sfdc_opportunity_xf.account_id
        left join
            churn_metrics
            on churn_metrics.opportunity_id = sfdc_opportunity_xf.opportunity_id

        where  -- remove test account
            sfdc_accounts_xf.ultimate_parent_account_id not in ('0016100001YUkWVAA1')
            -- remove test account
            and sfdc_opportunity_xf.account_id not in ('0014M00001kGcORQA0')
            and sfdc_opportunity_xf.is_deleted = 0

    ),
    add_calculated_net_arr_to_opty_final as (

        select
            oppty_final.*,

            -- -------------------------------------------------------------------------------------------
            -- -------------------------------------------------------------------------------------------
            -- I am faking that using the upper CTE, that should be replaced by the
            -- official table
            coalesce(
                net_iacv_to_net_arr_ratio.ratio_net_iacv_to_net_arr, 0
            ) as segment_order_type_iacv_to_net_arr_ratio,

            -- calculated net_arr
            -- uses ratios to estimate the net_arr based on iacv if open or net_iacv
            -- if closed
            -- NUANCE: Lost deals might not have net_incremental_acv populated, so we
            -- must rely on iacv
            -- Using opty ratio for open deals doesn't seem to work well
            case
                when  -- OPEN DEAL
                    oppty_final.stage_name not in (
                        '8-Closed Lost', '9-Unqualified', 'Closed Won', '10-Duplicate'
                    )
                then
                    coalesce(oppty_final.incremental_acv, 0)
                    * coalesce(segment_order_type_iacv_to_net_arr_ratio, 0)
                when  -- CLOSED LOST DEAL and no Net IACV
                    oppty_final.stage_name in ('8-Closed Lost')
                    and coalesce(oppty_final.net_incremental_acv, 0) = 0
                then
                    coalesce(oppty_final.incremental_acv, 0)
                    * coalesce(segment_order_type_iacv_to_net_arr_ratio, 0)
                -- REST of CLOSED DEAL
                when oppty_final.stage_name in ('8-Closed Lost', 'Closed Won')
                then
                    coalesce(oppty_final.net_incremental_acv, 0)
                    * coalesce(segment_order_type_iacv_to_net_arr_ratio, 0)
                else null
            end as calculated_from_ratio_net_arr,

            -- Calculated NET ARR is only used for deals closed earlier than FY19 and
            -- that have no raw_net_arr
            case
                when
                    oppty_final.close_date < '2018-02-01'::date
                    and coalesce(oppty_final.raw_net_arr, 0) = 0
                then calculated_from_ratio_net_arr
                -- Rest of deals after cut off date
                else coalesce(oppty_final.raw_net_arr, 0)
            end as net_arr,

            -- -------------------------------------------------------------------------------------------
            -- -------------------------------------------------------------------------------------------
            -- current deal size field, it was creasted by the data team and the
            -- original doesn't work
            case
                when net_arr > 0 and net_arr < 5000
                then '1 - Small (<5k)'
                when net_arr >= 5000 and net_arr < 25000
                then '2 - Medium (5k - 25k)'
                when net_arr >= 25000 and net_arr < 100000
                then '3 - Big (25k - 100k)'
                when net_arr >= 100000
                then '4 - Jumbo (>100k)'
                else 'Other'
            end as deal_size,

            -- extended version of the deal size
            case
                when net_arr > 0 and net_arr < 1000
                then '1. (0k -1k)'
                when net_arr >= 1000 and net_arr < 10000
                then '2. (1k - 10k)'
                when net_arr >= 10000 and net_arr < 50000
                then '3. (10k - 50k)'
                when net_arr >= 50000 and net_arr < 100000
                then '4. (50k - 100k)'
                when net_arr >= 100000 and net_arr < 250000
                then '5. (100k - 250k)'
                when net_arr >= 250000 and net_arr < 500000
                then '6. (250k - 500k)'
                when net_arr >= 500000 and net_arr < 1000000
                then '7. (500k-1000k)'
                when net_arr >= 1000000
                then '8. (>1000k)'
                else 'Other'
            end as calculated_deal_size,

            -- calculated age field
            -- if open, use the diff between created date and snapshot date
            -- if closed, a) the close date is later than snapshot date, use snapshot
            -- date
            -- if closed, b) the close is in the past, use close date
            case
                when oppty_final.is_open = 1
                then datediff(days, oppty_final.created_date, current_date)
                else datediff(days, oppty_final.created_date, oppty_final.close_date)
            end as calculated_age_in_days,

            -- Open pipeline eligibility definition
            case
                when
                    oppty_final.deal_group in ('1. New', '2. Growth')
                    and oppty_final.is_edu_oss = 0
                    and oppty_final.is_stage_1_plus = 1
                    and oppty_final.forecast_category_name != 'Omitted'
                    and oppty_final.is_open = 1
                then 1
                else 0
            end as is_eligible_open_pipeline_flag,


            -- Created pipeline eligibility definition
            -- https://gitlab.com/gitlab-com/sales-team/field-operations/systems/-/issues/2389
            case
                when
                    oppty_final.order_type_stamped
                    in ('1. New - First Order', '2. New - Connected', '3. Growth')
                    and oppty_final.is_edu_oss = 0
                    and oppty_final.pipeline_created_fiscal_quarter_date is not null
                    and oppty_final.opportunity_category in (
                        'Standard',
                        'Internal Correction',
                        'Ramp Deal',
                        'Credit',
                        'Contract Reset'
                    )
                    -- 20211222 Adjusted to remove the ommitted filter
                    and oppty_final.stage_name not in (
                        '00-Pre Opportunity',
                        '10-Duplicate',
                        '9-Unqualified',
                        '0-Pending Acceptance'
                    )
                    and (net_arr > 0 or oppty_final.opportunity_category = 'Credit')
                    -- 20220128 Updated to remove webdirect SQS deals 
                    and oppty_final.sales_qualified_source != 'Web Direct Generated'
                    and oppty_final.is_jihu_account = 0
                then 1
                else 0
            end as is_eligible_created_pipeline_flag,


            -- SAO alignment issue:
            -- https://gitlab.com/gitlab-com/sales-team/field-operations/sales-operations/-/issues/2656
            case
                when
                    oppty_final.sales_accepted_date is not null
                    and oppty_final.is_edu_oss = 0
                    and oppty_final.is_deleted = 0
                    and oppty_final.is_renewal = 0
                    and oppty_final.stage_name not in (
                        '00-Pre Opportunity',
                        '10-Duplicate',
                        '9-Unqualified',
                        '0-Pending Acceptance'
                    )
                then 1
                else 0
            end as is_eligible_sao_flag,

            -- ASP Analysis eligibility issue:
            -- https://gitlab.com/gitlab-com/sales-team/field-operations/sales-operations/-/issues/2606
            case
                when
                    oppty_final.is_edu_oss = 0
                    and oppty_final.is_deleted = 0
                    -- For ASP we care mainly about add on, new business, excluding
                    -- contraction / churn
                    and oppty_final.order_type_stamped
                    in ('1. New - First Order', '2. New - Connected', '3. Growth')
                    -- Exclude Decomissioned as they are not aligned to the real owner
                    -- Contract Reset, Decomission
                    and oppty_final.opportunity_category
                    in ('Standard', 'Ramp Deal', 'Internal Correction')
                    -- Exclude Deals with nARR < 0
                    and net_arr > 0
                then 1
                else 0
            end as is_eligible_asp_analysis_flag,

            -- Age eligibility issue:
            -- https://gitlab.com/gitlab-com/sales-team/field-operations/sales-operations/-/issues/2606
            case
                when
                    oppty_final.is_edu_oss = 0
                    and oppty_final.is_deleted = 0
                    -- Renewals are not having the same motion as rest of deals
                    and oppty_final.is_renewal = 0
                    -- For stage age we exclude only ps/other
                    and oppty_final.order_type_stamped in (
                        '1. New - First Order',
                        '2. New - Connected',
                        '3. Growth',
                        '4. Contraction',
                        '6. Churn - Final',
                        '5. Churn - Partial'
                    )
                    -- Only include deal types with meaningful journeys through the
                    -- stages
                    and oppty_final.opportunity_category
                    in ('Standard', 'Ramp Deal', 'Decommissioned')
                    -- Web Purchase have a different dynamic and should not be included
                    and oppty_final.is_web_portal_purchase = 0
                then 1
                else 0
            end as is_eligible_age_analysis_flag,

            case
                when
                    oppty_final.is_edu_oss = 0
                    and oppty_final.is_deleted = 0
                    and (
                        oppty_final.is_won = 1
                        or (oppty_final.is_renewal = 1 and oppty_final.is_lost = 1)
                    )
                    and oppty_final.order_type_stamped in (
                        '1. New - First Order',
                        '2. New - Connected',
                        '3. Growth',
                        '4. Contraction',
                        '6. Churn - Final',
                        '5. Churn - Partial'
                    )
                then 1
                else 0
            end as is_booked_net_arr_flag,

            case
                when
                    oppty_final.is_edu_oss = 0
                    and oppty_final.is_deleted = 0
                    and oppty_final.order_type_stamped
                    in ('4. Contraction', '6. Churn - Final', '5. Churn - Partial')
                then 1
                else 0
            end as is_eligible_churn_contraction_flag,

            -- compound metrics to facilitate reporting
            -- created and closed within the quarter net arr
            case
                when
                    oppty_final.pipeline_created_fiscal_quarter_date
                    = oppty_final.close_fiscal_quarter_date
                    and is_eligible_created_pipeline_flag = 1
                then net_arr
                else 0
            end as created_and_won_same_quarter_net_arr,

            -- -------------------------------------------------------------------------------------------------------
            -- -------------------------------------------------------------------------------------------------------
            -- Fields created to simplify report building down the road. Specially the
            -- pipeline velocity.
            -- deal count
            case
                when
                    is_eligible_open_pipeline_flag = 1
                    and oppty_final.is_stage_1_plus = 1
                then oppty_final.calculated_deal_count
                else 0
            end as open_1plus_deal_count,

            case
                when
                    is_eligible_open_pipeline_flag = 1
                    and oppty_final.is_stage_3_plus = 1
                then oppty_final.calculated_deal_count
                else 0
            end as open_3plus_deal_count,

            case
                when
                    is_eligible_open_pipeline_flag = 1
                    and oppty_final.is_stage_4_plus = 1
                then oppty_final.calculated_deal_count
                else 0
            end as open_4plus_deal_count,

            -- booked deal count
            case
                when oppty_final.is_won = 1
                then oppty_final.calculated_deal_count
                else 0
            end as booked_deal_count,

            -- churned contraction deal count as OT
            case
                when is_eligible_churn_contraction_flag = 1
                then oppty_final.calculated_deal_count
                else 0
            end as churned_contraction_deal_count,


            case
                when
                    (
                        (oppty_final.is_renewal = 1 and oppty_final.is_lost = 1)
                        or oppty_final.is_won = 1
                    )
                    and is_eligible_churn_contraction_flag = 1
                then oppty_final.calculated_deal_count
                else 0
            end as booked_churned_contraction_deal_count,
            -- ---------------
            -- Net ARR
            case
                when is_eligible_open_pipeline_flag = 1 then net_arr else 0
            end as open_1plus_net_arr,

            case
                when
                    is_eligible_open_pipeline_flag = 1
                    and oppty_final.is_stage_3_plus = 1
                then net_arr
                else 0
            end as open_3plus_net_arr,

            case
                when
                    is_eligible_open_pipeline_flag = 1
                    and oppty_final.is_stage_4_plus = 1
                then net_arr
                else 0
            end as open_4plus_net_arr,

            -- booked net arr (won + renewals / lost)
            case
                when
                    (
                        oppty_final.is_won = 1
                        or (oppty_final.is_renewal = 1 and oppty_final.is_lost = 1)
                    )
                then net_arr
                else 0
            end as booked_net_arr,

            -- booked churned contraction net arr as OT
            case
                when
                    (
                        (oppty_final.is_renewal = 1 and oppty_final.is_lost = 1)
                        or oppty_final.is_won = 1
                    )
                    and is_eligible_churn_contraction_flag = 1
                then net_arr
                else 0
            end as booked_churned_contraction_net_arr,

            -- churned contraction net arr as OT
            case
                when is_eligible_churn_contraction_flag = 1 then net_arr else 0
            end as churned_contraction_net_arr,

            case
                when net_arr > -5000 and is_eligible_churn_contraction_flag = 1
                then '1. < 5k'
                when
                    net_arr > -20000
                    and net_arr <= -5000
                    and is_eligible_churn_contraction_flag = 1
                then '2. 5k-20k'
                when
                    net_arr > -50000
                    and net_arr <= -20000
                    and is_eligible_churn_contraction_flag = 1
                then '3. 20k-50k'
                when
                    net_arr > -100000
                    and net_arr <= -50000
                    and is_eligible_churn_contraction_flag = 1
                then '4. 50k-100k'
                when net_arr < -100000 and is_eligible_churn_contraction_flag = 1
                then '5. 100k+'
            end as churn_contracton_net_arr_bucket,

            -- NF 2022-02-17 These keys are used in the pipeline metrics models and on
            -- the X-Ray dashboard to link gSheets with
            -- different aggregation levels
            coalesce(agg_demo_keys.key_sqs, 'other') as key_sqs,
            coalesce(agg_demo_keys.key_ot, 'other') as key_ot,

            coalesce(agg_demo_keys.key_segment, 'other') as key_segment,
            coalesce(agg_demo_keys.key_segment_sqs, 'other') as key_segment_sqs,
            coalesce(agg_demo_keys.key_segment_ot, 'other') as key_segment_ot,

            coalesce(agg_demo_keys.key_segment_geo, 'other') as key_segment_geo,
            coalesce(agg_demo_keys.key_segment_geo_sqs, 'other') as key_segment_geo_sqs,
            coalesce(agg_demo_keys.key_segment_geo_ot, 'other') as key_segment_geo_ot,

            coalesce(
                agg_demo_keys.key_segment_geo_region, 'other'
            ) as key_segment_geo_region,
            coalesce(
                agg_demo_keys.key_segment_geo_region_sqs, 'other'
            ) as key_segment_geo_region_sqs,
            coalesce(
                agg_demo_keys.key_segment_geo_region_ot, 'other'
            ) as key_segment_geo_region_ot,

            coalesce(
                agg_demo_keys.key_segment_geo_region_area, 'other'
            ) as key_segment_geo_region_area,
            coalesce(
                agg_demo_keys.key_segment_geo_region_area_sqs, 'other'
            ) as key_segment_geo_region_area_sqs,
            coalesce(
                agg_demo_keys.key_segment_geo_region_area_ot, 'other'
            ) as key_segment_geo_region_area_ot,

            coalesce(
                agg_demo_keys.key_segment_geo_area, 'other'
            ) as key_segment_geo_area,

            coalesce(
                agg_demo_keys.report_opportunity_user_segment, 'other'
            ) as sales_team_cro_level,

            -- NF: This code replicates the reporting structured of FY22, to keep
            -- current tools working
            coalesce(
                agg_demo_keys.sales_team_rd_asm_level, 'other'
            ) as sales_team_rd_asm_level,

            coalesce(agg_demo_keys.sales_team_vp_level, 'other') as sales_team_vp_level,
            coalesce(
                agg_demo_keys.sales_team_avp_rd_level, 'other'
            ) as sales_team_avp_rd_level,
            coalesce(
                agg_demo_keys.sales_team_asm_level, 'other'
            ) as sales_team_asm_level


        from oppty_final
        -- Net IACV to Net ARR conversion table
        left join
            net_iacv_to_net_arr_ratio
            on net_iacv_to_net_arr_ratio.user_segment_stamped
            = oppty_final.opportunity_owner_user_segment
            and net_iacv_to_net_arr_ratio.order_type_stamped
            = oppty_final.order_type_stamped
        -- Add keys for aggregated analysis
        left join
            agg_demo_keys
            on oppty_final.report_user_segment_geo_region_area_sqs_ot
            = agg_demo_keys.report_user_segment_geo_region_area_sqs_ot

    )
select *
from add_calculated_net_arr_to_opty_final

{{ config(alias="sfdc_opportunity_snapshot_history_xf") }}
-- TODO
-- Add CS churn fields into model from wk_sales_opportunity object
with
    date_details as (select * from {{ ref("wk_sales_date_details") }}),
    sfdc_accounts_xf as (select * from {{ ref("sfdc_accounts_xf") }}),
    sfdc_opportunity_xf as (

        select
            opportunity_id,
            owner_id,
            account_id,
            order_type_stamped,
            deal_category,
            opportunity_category,
            deal_group,
            opportunity_owner_manager,
            is_edu_oss,
            account_owner_team_stamped,

            -- Opportunity Owner Stamped fields
            opportunity_owner_user_segment,
            opportunity_owner_user_region,
            opportunity_owner_user_area,
            opportunity_owner_user_geo,

            -- -----------------
            -- NF 2022-01-28 TO BE DEPRECATED once pipeline velocity reports in
            -- Sisense are updated
            sales_team_rd_asm_level,
            -- -----------------
            sales_team_cro_level,
            sales_team_vp_level,
            sales_team_avp_rd_level,
            sales_team_asm_level,

            -- this fields use the opportunity owner version for current FY and
            -- account fields for previous years
            report_opportunity_user_segment,
            report_opportunity_user_geo,
            report_opportunity_user_region,
            report_opportunity_user_area,
            report_user_segment_geo_region_area,
            report_user_segment_geo_region_area_sqs_ot,

            -- NF 2022-02-17 new aggregated keys 
            key_sqs,
            key_ot,

            key_segment,
            key_segment_sqs,
            key_segment_ot,

            key_segment_geo,
            key_segment_geo_sqs,
            key_segment_geo_ot,

            key_segment_geo_region,
            key_segment_geo_region_sqs,
            key_segment_geo_region_ot,

            key_segment_geo_region_area,
            key_segment_geo_region_area_sqs,
            key_segment_geo_region_area_ot,

            key_segment_geo_area,

            -- -----------------------------------
            -- NF: These fields are not exposed yet in opty history, just for check
            -- I am adding this logic
            stage_1_date,
            stage_1_date_month,
            stage_1_fiscal_year,
            stage_1_fiscal_quarter_name,
            stage_1_fiscal_quarter_date,
            -- ------------------------------------
            is_won,
            is_duplicate_flag,
            raw_net_arr,
            net_incremental_acv,
            sales_qualified_source,
            incremental_acv

        -- Channel Org. fields
        -- this fields should be changed to this historical version
        -- deal_path,
        -- dr_partner_deal_type,
        -- dr_partner_engagement,
        -- partner_account,
        -- dr_status,
        -- distributor,
        -- influence_partner,
        -- fulfillment_partner,
        -- platform_partner,
        -- partner_track,
        -- is_public_sector_opp,
        -- is_registration_from_portal,
        -- calculated_discount,
        -- partner_discount,
        -- partner_discount_calc,
        -- comp_channel_neutral
        from {{ ref("wk_sales_sfdc_opportunity_xf") }}

    ),
    sfdc_users_xf as (select * from {{ ref("wk_sales_sfdc_users_xf") }}),
    sfdc_opportunity_snapshot_history as (

        select
            -- sfdc_opportunity_snapshot_history.valid_from,
            -- sfdc_opportunity_snapshot_history.valid_to,
            -- sfdc_opportunity_snapshot_history.is_currently_valid,
            sfdc_opportunity_snapshot_history.opportunity_snapshot_id,

            -- Accounts might get deleted or merged, I am selecting the latest account
            -- id from the opty object
            -- to avoid showing non-valid account ids
            sfdc_opportunity_snapshot_history.account_id as raw_account_id,

            sfdc_opportunity_snapshot_history.opportunity_id,
            sfdc_opportunity_snapshot_history.opportunity_name,
            sfdc_opportunity_snapshot_history.owner_id,
            -- sfdc_opportunity_snapshot_history.business_type,
            sfdc_opportunity_snapshot_history.close_date,
            sfdc_opportunity_snapshot_history.created_date,
            sfdc_opportunity_snapshot_history.deployment_preference,
            -- sfdc_opportunity_snapshot_history.generated_source,
            sfdc_opportunity_snapshot_history.lead_source,
            sfdc_opportunity_snapshot_history.merged_opportunity_id,

            -- NF: This field is added directly from the user table
            -- as the opportunity one is not clean
            -- sfdc_opportunity_snapshot_history.opportunity_owner,
            sfdc_opportunity_snapshot_history.opportunity_owner_department,
            sfdc_opportunity_snapshot_history.opportunity_sales_development_representative,
            sfdc_opportunity_snapshot_history.opportunity_business_development_representative,
            sfdc_opportunity_snapshot_history.opportunity_development_representative,

            sfdc_opportunity_snapshot_history.order_type_stamped
            as snapshot_order_type_stamped,
            -- sfdc_opportunity_snapshot_history.order_type,
            -- sfdc_opportunity_snapshot_history.opportunity_owner_manager,
            -- sfdc_opportunity_snapshot_history.account_owner_team_stamped,
            -- sfdc_opportunity_snapshot_history.parent_segment,
            sfdc_opportunity_snapshot_history.sales_accepted_date,
            sfdc_opportunity_snapshot_history.sales_path,
            sfdc_opportunity_snapshot_history.sales_qualified_date,
            -- sfdc_opportunity_snapshot_history.sales_segment,
            sfdc_opportunity_snapshot_history.sales_type,
            sfdc_opportunity_snapshot_history.net_new_source_categories,
            sfdc_opportunity_snapshot_history.source_buckets,
            sfdc_opportunity_snapshot_history.stage_name,

            sfdc_opportunity_snapshot_history.acv,
            -- sfdc_opportunity_snapshot_history.closed_deals,
            sfdc_opportunity_snapshot_history.competitors,
            -- sfdc_opportunity_snapshot_history.critical_deal_flag,
            -- sfdc_opportunity_snapshot_history.deal_size,
            sfdc_opportunity_snapshot_history.forecast_category_name,
            -- sfdc_opportunity_snapshot_history.forecasted_iacv,
            sfdc_opportunity_snapshot_history.iacv_created_date,
            sfdc_opportunity_snapshot_history.incremental_acv,
            sfdc_opportunity_snapshot_history.invoice_number,

            -- logic needs to be added here once the oppotunity category fields is
            -- merged
            -- https://gitlab.com/gitlab-data/analytics/-/issues/7888
            case
                when
                    sfdc_opportunity_snapshot_history.opportunity_category
                    in ('Decommission')
                then 1
                else 0
            end as is_refund,

            case
                when
                    sfdc_opportunity_snapshot_history.opportunity_category in ('Credit')
                then 1
                else 0
            end as is_credit_flag,

            case
                when
                    sfdc_opportunity_snapshot_history.opportunity_category
                    in ('Contract Reset')
                then 1
                else 0
            end as is_contract_reset_flag,
            -- sfdc_opportunity_snapshot_history.is_refund,
            -- sfdc_opportunity_snapshot_history.is_downgrade,
            -- sfdc_opportunity_snapshot_history.is_swing_deal,
            sfdc_opportunity_snapshot_history.net_incremental_acv,
            -- sfdc_opportunity_snapshot_history.nrv,
            sfdc_opportunity_snapshot_history.primary_campaign_source_id,
            -- sfdc_opportunity_snapshot_history.probability,
            sfdc_opportunity_snapshot_history.professional_services_value,
            -- sfdc_opportunity_snapshot_history.pushed_count,
            -- sfdc_opportunity_snapshot_history.reason_for_loss,
            -- sfdc_opportunity_snapshot_history.reason_for_loss_details,
            sfdc_opportunity_snapshot_history.refund_iacv,
            sfdc_opportunity_snapshot_history.downgrade_iacv,
            sfdc_opportunity_snapshot_history.renewal_acv,
            sfdc_opportunity_snapshot_history.renewal_amount,
            sfdc_opportunity_snapshot_history.sales_qualified_source
            as snapshot_sales_qualified_source,
            sfdc_opportunity_snapshot_history.is_edu_oss as snapshot_is_edu_oss,

            -- sfdc_opportunity_snapshot_history.segment,
            -- sfdc_opportunity_snapshot_history.solutions_to_be_replaced,
            sfdc_opportunity_snapshot_history.total_contract_value,
            -- sfdc_opportunity_snapshot_history.upside_iacv,
            -- sfdc_opportunity_snapshot_history.upside_swing_deal_iacv,
            sfdc_opportunity_snapshot_history.is_web_portal_purchase,
            sfdc_opportunity_snapshot_history.opportunity_term,

            sfdc_opportunity_snapshot_history.net_arr as raw_net_arr,

            -- sfdc_opportunity_snapshot_history.user_segment_stamped,
            -- sfdc_opportunity_snapshot_history.user_region_stamped,
            -- sfdc_opportunity_snapshot_history.user_area_stamped,
            -- sfdc_opportunity_snapshot_history.user_geo_stamped,
            sfdc_opportunity_snapshot_history.arr_basis,
            sfdc_opportunity_snapshot_history.arr,
            sfdc_opportunity_snapshot_history.amount,
            sfdc_opportunity_snapshot_history.recurring_amount,
            sfdc_opportunity_snapshot_history.true_up_amount,
            sfdc_opportunity_snapshot_history.proserv_amount,
            sfdc_opportunity_snapshot_history.other_non_recurring_amount,
            sfdc_opportunity_snapshot_history.subscription_start_date,
            sfdc_opportunity_snapshot_history.subscription_end_date,
            /*
      sfdc_opportunity_snapshot_history.cp_champion,
      sfdc_opportunity_snapshot_history.cp_close_plan,
      sfdc_opportunity_snapshot_history.cp_competition,
      sfdc_opportunity_snapshot_history.cp_decision_criteria,
      sfdc_opportunity_snapshot_history.cp_decision_process,
      sfdc_opportunity_snapshot_history.cp_economic_buyer,
      sfdc_opportunity_snapshot_history.cp_identify_pain,
      sfdc_opportunity_snapshot_history.cp_metrics,
      sfdc_opportunity_snapshot_history.cp_risks,
      */
            sfdc_opportunity_snapshot_history.cp_use_cases,
            /*sfdc_opportunity_snapshot_history.cp_value_driver,
      sfdc_opportunity_snapshot_history.cp_why_do_anything_at_all,
      sfdc_opportunity_snapshot_history.cp_why_gitlab,
      sfdc_opportunity_snapshot_history.cp_why_now,
      */
            sfdc_opportunity_snapshot_history._last_dbt_run,
            sfdc_opportunity_snapshot_history.is_deleted,
            sfdc_opportunity_snapshot_history.last_activity_date,
            sfdc_opportunity_snapshot_history.record_type_id,
            -- sfdc_opportunity_snapshot_history.opportunity_category,
            -- Channel Org. fields
            -- this fields should be changed to this historical version
            sfdc_opportunity_snapshot_history.deal_path,
            sfdc_opportunity_snapshot_history.dr_partner_deal_type,
            sfdc_opportunity_snapshot_history.dr_partner_engagement,
            sfdc_opportunity_snapshot_history.partner_account,
            sfdc_opportunity_snapshot_history.dr_status,
            sfdc_opportunity_snapshot_history.distributor,
            sfdc_opportunity_snapshot_history.influence_partner,
            sfdc_opportunity_snapshot_history.fulfillment_partner,
            sfdc_opportunity_snapshot_history.platform_partner,
            sfdc_opportunity_snapshot_history.partner_track,
            sfdc_opportunity_snapshot_history.is_public_sector_opp,
            sfdc_opportunity_snapshot_history.is_registration_from_portal,
            sfdc_opportunity_snapshot_history.calculated_discount,
            sfdc_opportunity_snapshot_history.partner_discount,
            sfdc_opportunity_snapshot_history.partner_discount_calc,
            sfdc_opportunity_snapshot_history.comp_channel_neutral,

            sfdc_opportunity_snapshot_history.fpa_master_bookings_flag,

            case
                when sfdc_opportunity_snapshot_history.deal_path = 'Direct'
                then 'Direct'
                when sfdc_opportunity_snapshot_history.deal_path = 'Web Direct'
                then 'Web Direct'
                when
                    sfdc_opportunity_snapshot_history.deal_path = 'Channel'
                    and sfdc_opportunity_snapshot_history.sales_qualified_source
                    = 'Channel Generated'
                then 'Partner Sourced'
                when
                    sfdc_opportunity_snapshot_history.deal_path = 'Channel'
                    and sfdc_opportunity_snapshot_history.sales_qualified_source
                    != 'Channel Generated'
                then 'Partner Co-Sell'
            end as deal_path_engagement,

            -- stage dates
            -- dates in stage fields
            sfdc_opportunity_snapshot_history.stage_0_pending_acceptance_date,
            sfdc_opportunity_snapshot_history.stage_1_discovery_date,
            sfdc_opportunity_snapshot_history.stage_2_scoping_date,
            sfdc_opportunity_snapshot_history.stage_3_technical_evaluation_date,
            sfdc_opportunity_snapshot_history.stage_4_proposal_date,
            sfdc_opportunity_snapshot_history.stage_5_negotiating_date,
            sfdc_opportunity_snapshot_history.stage_6_awaiting_signature_date,
            sfdc_opportunity_snapshot_history.stage_6_closed_won_date,
            sfdc_opportunity_snapshot_history.stage_6_closed_lost_date,

            -- date helpers
            sfdc_opportunity_snapshot_history.date_actual as snapshot_date,
            snapshot_date.first_day_of_month as snapshot_date_month,
            snapshot_date.fiscal_year as snapshot_fiscal_year,
            snapshot_date.fiscal_quarter_name_fy as snapshot_fiscal_quarter_name,
            snapshot_date.first_day_of_fiscal_quarter as snapshot_fiscal_quarter_date,
            snapshot_date.day_of_fiscal_quarter_normalised
            as snapshot_day_of_fiscal_quarter_normalised,
            snapshot_date.day_of_fiscal_year_normalised
            as snapshot_day_of_fiscal_year_normalised,

            close_date_detail.first_day_of_month as close_date_month,
            close_date_detail.fiscal_year as close_fiscal_year,
            close_date_detail.fiscal_quarter_name_fy as close_fiscal_quarter_name,
            close_date_detail.first_day_of_fiscal_quarter as close_fiscal_quarter_date,

            -- This refers to the closing quarter perspective instead of the snapshot
            -- quarter
            90 - datediff(
                day,
                snapshot_date.date_actual,
                close_date_detail.last_day_of_fiscal_quarter
            ) as close_day_of_fiscal_quarter_normalised,

            created_date_detail.first_day_of_month as created_date_month,
            created_date_detail.fiscal_year as created_fiscal_year,
            created_date_detail.fiscal_quarter_name_fy as created_fiscal_quarter_name,
            created_date_detail.first_day_of_fiscal_quarter
            as created_fiscal_quarter_date,

            net_arr_created_date.first_day_of_month as iacv_created_date_month,
            net_arr_created_date.fiscal_year as iacv_created_fiscal_year,
            net_arr_created_date.fiscal_quarter_name_fy
            as iacv_created_fiscal_quarter_name,
            net_arr_created_date.first_day_of_fiscal_quarter
            as iacv_created_fiscal_quarter_date,

            created_date_detail.date_actual as net_arr_created_date,
            created_date_detail.first_day_of_month as net_arr_created_date_month,
            created_date_detail.fiscal_year as net_arr_created_fiscal_year,
            created_date_detail.fiscal_quarter_name_fy
            as net_arr_created_fiscal_quarter_name,
            created_date_detail.first_day_of_fiscal_quarter
            as net_arr_created_fiscal_quarter_date,

            net_arr_created_date.date_actual as pipeline_created_date,
            net_arr_created_date.first_day_of_month as pipeline_created_date_month,
            net_arr_created_date.fiscal_year as pipeline_created_fiscal_year,
            net_arr_created_date.fiscal_quarter_name_fy
            as pipeline_created_fiscal_quarter_name,
            net_arr_created_date.first_day_of_fiscal_quarter
            as pipeline_created_fiscal_quarter_date,

            sales_accepted_date.first_day_of_month as sales_accepted_month,
            sales_accepted_date.fiscal_year as sales_accepted_fiscal_year,
            sales_accepted_date.fiscal_quarter_name_fy
            as sales_accepted_fiscal_quarter_name,
            sales_accepted_date.first_day_of_fiscal_quarter
            as sales_accepted_fiscal_quarter_date,
            -- ----------------------------------------------------------------------------------------------------
            -- ----------------------------------------------------------------------------------------------------
            -- Base helpers for reporting
            case
                when
                    sfdc_opportunity_snapshot_history.stage_name in (
                        '00-Pre Opportunity',
                        '0-Pending Acceptance',
                        '0-Qualifying',
                        'Developing',
                        '1-Discovery',
                        '2-Developing',
                        '2-Scoping'
                    )
                then 'Pipeline'
                when
                    sfdc_opportunity_snapshot_history.stage_name in (
                        '3-Technical Evaluation',
                        '4-Proposal',
                        '5-Negotiating',
                        '6-Awaiting Signature',
                        '7-Closing'
                    )
                then '3+ Pipeline'
                when
                    sfdc_opportunity_snapshot_history.stage_name
                    in ('8-Closed Lost', 'Closed Lost')
                then 'Lost'
                when sfdc_opportunity_snapshot_history.stage_name in ('Closed Won')
                then 'Closed Won'
                else 'Other'
            end as stage_name_3plus,

            case
                when
                    sfdc_opportunity_snapshot_history.stage_name in (
                        '00-Pre Opportunity',
                        '0-Pending Acceptance',
                        '0-Qualifying',
                        'Developing',
                        '1-Discovery',
                        '2-Developing',
                        '2-Scoping',
                        '3-Technical Evaluation'
                    )
                then 'Pipeline'
                when
                    sfdc_opportunity_snapshot_history.stage_name in (
                        '4-Proposal',
                        '5-Negotiating',
                        '6-Awaiting Signature',
                        '7-Closing'
                    )
                then '4+ Pipeline'
                when
                    sfdc_opportunity_snapshot_history.stage_name
                    in ('8-Closed Lost', 'Closed Lost')
                then 'Lost'
                when sfdc_opportunity_snapshot_history.stage_name in ('Closed Won')
                then 'Closed Won'
                else 'Other'
            end as stage_name_4plus,


            case
                when
                    sfdc_opportunity_snapshot_history.stage_name in (
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
                    sfdc_opportunity_snapshot_history.stage_name in (
                        '3-Technical Evaluation',
                        '4-Proposal',
                        'Closed Won',
                        '5-Negotiating',
                        '6-Awaiting Signature',
                        '7-Closing'
                    )
                then 1
                else 0
            end as is_stage_3_plus,

            case
                when
                    sfdc_opportunity_snapshot_history.stage_name in (
                        '4-Proposal',
                        'Closed Won',
                        '5-Negotiating',
                        '6-Awaiting Signature',
                        '7-Closing'
                    )
                then 1
                else 0
            end as is_stage_4_plus,

            case
                when sfdc_opportunity_snapshot_history.stage_name = 'Closed Won'
                then 1
                else 0
            end as is_won,

            case
                when
                    sfdc_opportunity_snapshot_history.stage_name
                    in ('8-Closed Lost', 'Closed Lost')
                then 1
                else 0
            end as is_lost,

            case
                when
                    sfdc_opportunity_snapshot_history.stage_name in (
                        '8-Closed Lost',
                        'Closed Lost',
                        '9-Unqualified',
                        'Closed Won',
                        '10-Duplicate'
                    )
                then 0
                else 1
            end as is_open,

            case
                when
                    sfdc_opportunity_snapshot_history.stage_name in (
                        '8-Closed Lost',
                        'Closed Lost',
                        '9-Unqualified',
                        'Closed Won',
                        '10-Duplicate'
                    )
                then 1
                else 0
            end as is_closed,


            case
                when
                    lower(sfdc_opportunity_snapshot_history.sales_type) like '%renewal%'
                then 1
                else 0
            end as is_renewal,


            -- NF: 20210827 Fields for competitor analysis 
            case
                when contains(sfdc_opportunity_snapshot_history.competitors, 'Other')
                then 1
                else 0
            end as competitors_other_flag,
            case
                when
                    contains(
                        sfdc_opportunity_snapshot_history.competitors, 'GitLab Core'
                    )
                then 1
                else 0
            end as competitors_gitlab_core_flag,
            case
                when contains(sfdc_opportunity_snapshot_history.competitors, 'None')
                then 1
                else 0
            end as competitors_none_flag,
            case
                when
                    contains(
                        sfdc_opportunity_snapshot_history.competitors,
                        'GitHub Enterprise'
                    )
                then 1
                else 0
            end as competitors_github_enterprise_flag,
            case
                when
                    contains(
                        sfdc_opportunity_snapshot_history.competitors,
                        'BitBucket Server'
                    )
                then 1
                else 0
            end as competitors_bitbucket_server_flag,
            case
                when contains(sfdc_opportunity_snapshot_history.competitors, 'Unknown')
                then 1
                else 0
            end as competitors_unknown_flag,
            case
                when
                    contains(
                        sfdc_opportunity_snapshot_history.competitors, 'GitHub.com'
                    )
                then 1
                else 0
            end as competitors_github_flag,
            case
                when
                    contains(
                        sfdc_opportunity_snapshot_history.competitors, 'GitLab.com'
                    )
                then 1
                else 0
            end as competitors_gitlab_flag,
            case
                when contains(sfdc_opportunity_snapshot_history.competitors, 'Jenkins')
                then 1
                else 0
            end as competitors_jenkins_flag,
            case
                when
                    contains(
                        sfdc_opportunity_snapshot_history.competitors, 'Azure DevOps'
                    )
                then 1
                else 0
            end as competitors_azure_devops_flag,
            case
                when contains(sfdc_opportunity_snapshot_history.competitors, 'SVN')
                then 1
                else 0
            end as competitors_svn_flag,
            case
                when
                    contains(
                        sfdc_opportunity_snapshot_history.competitors, 'BitBucket.Org'
                    )
                then 1
                else 0
            end as competitors_bitbucket_flag,
            case
                when
                    contains(sfdc_opportunity_snapshot_history.competitors, 'Atlassian')
                then 1
                else 0
            end as competitors_atlassian_flag,
            case
                when contains(sfdc_opportunity_snapshot_history.competitors, 'Perforce')
                then 1
                else 0
            end as competitors_perforce_flag,
            case
                when
                    contains(
                        sfdc_opportunity_snapshot_history.competitors,
                        'Visual Studio Team Services'
                    )
                then 1
                else 0
            end as competitors_visual_studio_flag,
            case
                when contains(sfdc_opportunity_snapshot_history.competitors, 'Azure')
                then 1
                else 0
            end as competitors_azure_flag,
            case
                when
                    contains(
                        sfdc_opportunity_snapshot_history.competitors,
                        'Amazon Code Commit'
                    )
                then 1
                else 0
            end as competitors_amazon_code_commit_flag,
            case
                when contains(sfdc_opportunity_snapshot_history.competitors, 'CircleCI')
                then 1
                else 0
            end as competitors_circleci_flag,
            case
                when contains(sfdc_opportunity_snapshot_history.competitors, 'Bamboo')
                then 1
                else 0
            end as competitors_bamboo_flag,
            case
                when contains(sfdc_opportunity_snapshot_history.competitors, 'AWS')
                then 1
                else 0
            end as competitors_aws_flag,


            -- calculated age field
            -- if open, use the diff between created date and snapshot date
            -- if closed, a) the close date is later than snapshot date, use snapshot
            -- date
            -- if closed, b) the close is in the past, use close date
            case
                when is_open = 1
                then
                    datediff(
                        days, created_date_detail.date_actual, snapshot_date.date_actual
                    )
                when
                    is_open = 0
                    and snapshot_date.date_actual < close_date_detail.date_actual
                then
                    datediff(
                        days, created_date_detail.date_actual, snapshot_date.date_actual
                    )
                else
                    datediff(
                        days,
                        created_date_detail.date_actual,
                        close_date_detail.date_actual
                    )
            end as calculated_age_in_days

        from {{ ref("sfdc_opportunity_snapshot_history") }}
        inner join
            date_details close_date_detail
            on close_date_detail.date_actual
            = sfdc_opportunity_snapshot_history.close_date::date
        inner join
            date_details snapshot_date
            on sfdc_opportunity_snapshot_history.date_actual::date
            = snapshot_date.date_actual
        left join
            date_details created_date_detail
            on created_date_detail.date_actual
            = sfdc_opportunity_snapshot_history.created_date::date
        left join
            date_details net_arr_created_date
            on net_arr_created_date.date_actual
            = sfdc_opportunity_snapshot_history.iacv_created_date::date
        left join
            date_details sales_accepted_date
            on sales_accepted_date.date_actual
            = sfdc_opportunity_snapshot_history.sales_accepted_date::date


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
    sfdc_opportunity_snapshot_history_xf as (

        select distinct

            opp_snapshot.*,

            case
                when opp_snapshot.is_won = 1
                then '1.Won'
                when opp_snapshot.is_lost = 1
                then '2.Lost'
                when opp_snapshot.is_open = 1
                then '0. Open'
                else 'N/A'
            end as stage_category,
            -- ----------------------------------------------------------------------------------------------------
            -- ----------------------------------------------------------------------------------------------------
            -- Historical Net ARR Logic Summary   
            -- closed deals use net_incremental_acv
            -- open deals use incremental acv
            -- closed won deals with net_arr > 0 use that opportunity calculated ratio
            -- deals with no opty with net_arr use a default ratio for segment / order
            -- type
            -- deals before 2021-02-01 use always net_arr calculated from ratio
            -- deals after 2021-02-01 use net_arr if > 0, if open and not net_arr uses
            -- ratio version
            -- If the opportunity exists, use the ratio from the opportunity sheetload
            -- I am faking that using the opportunity table directly
            case
                when  -- only consider won deals
                    sfdc_opportunity_xf.is_won = 1
                    -- contract resets have a special way of calculating net iacv
                    and sfdc_opportunity_xf.opportunity_category <> 'Contract Reset'
                    and coalesce(sfdc_opportunity_xf.raw_net_arr, 0) <> 0
                    and coalesce(sfdc_opportunity_xf.net_incremental_acv, 0) <> 0
                then
                    coalesce(
                        sfdc_opportunity_xf.raw_net_arr
                        / sfdc_opportunity_xf.net_incremental_acv,
                        0
                    )
                else null
            end as opportunity_based_iacv_to_net_arr_ratio,

            -- If there is no opportnity, use a default table ratio
            -- I am faking that using the upper CTE, that should be replaced by the
            -- official table
            coalesce(
                net_iacv_to_net_arr_ratio.ratio_net_iacv_to_net_arr, 0
            ) as segment_order_type_iacv_to_net_arr_ratio,

            -- calculated net_arr
            -- uses ratios to estimate the net_arr based on iacv if open or net_iacv
            -- if closed
            -- if there is an opportunity based ratio, use that, if not, use default
            -- from segment / order type
            -- NUANCE: Lost deals might not have net_incremental_acv populated, so we
            -- must rely on iacv
            -- Using opty ratio for open deals doesn't seem to work well
            case
                when  -- OPEN DEAL
                    opp_snapshot.stage_name not in (
                        '8-Closed Lost', '9-Unqualified', 'Closed Won', '10-Duplicate'
                    )
                then
                    coalesce(opp_snapshot.incremental_acv, 0)
                    * coalesce(segment_order_type_iacv_to_net_arr_ratio, 0)
                when  -- CLOSED LOST DEAL and no Net IACV
                    opp_snapshot.stage_name in ('8-Closed Lost')
                    and coalesce(opp_snapshot.net_incremental_acv, 0) = 0
                then
                    coalesce(opp_snapshot.incremental_acv, 0)
                    * coalesce(segment_order_type_iacv_to_net_arr_ratio, 0)
                -- REST of CLOSED DEAL
                when opp_snapshot.stage_name in ('8-Closed Lost', 'Closed Won')
                then
                    coalesce(opp_snapshot.net_incremental_acv, 0) * coalesce(
                        opportunity_based_iacv_to_net_arr_ratio,
                        segment_order_type_iacv_to_net_arr_ratio
                    )
                else null
            end as calculated_from_ratio_net_arr,

            -- For opportunities before start of FY22, as Net ARR was WIP, there are a
            -- lot of opties with IACV or Net IACV and no Net ARR
            -- Those were later fixed in the opportunity object but stayed in the
            -- snapshot table.
            -- To account for those issues and give a directionally correct answer, we
            -- apply a ratio to everything before FY22
            case
                -- All deals before cutoff and that were not updated to Net ARR
                when opp_snapshot.snapshot_date < '2021-02-01'::date
                then calculated_from_ratio_net_arr
                -- After cutoff date, for all deals earlier than FY19 that are closed
                -- and have no net arr
                when
                    opp_snapshot.snapshot_date >= '2021-02-01'::date
                    and opp_snapshot.close_date < '2018-02-01'::date
                    and opp_snapshot.is_open = 0
                    and coalesce(opp_snapshot.raw_net_arr, 0) = 0
                then calculated_from_ratio_net_arr
                -- Rest of deals after cut off date
                else coalesce(opp_snapshot.raw_net_arr, 0)
            end as net_arr,

            -- ----------------------------
            -- fields for counting new logos, these fields count refund as negative
            case
                when opp_snapshot.is_refund = 1
                then -1
                when opp_snapshot.is_credit_flag = 1
                then 0
                else 1
            end as calculated_deal_count,

            -- ----------------------------------------------------------------------------------------------------
            -- ----------------------------------------------------------------------------------------------------
            -- opportunity driven fields
            sfdc_opportunity_xf.opportunity_owner_manager,
            sfdc_opportunity_xf.is_edu_oss,
            sfdc_opportunity_xf.sales_qualified_source,
            sfdc_opportunity_xf.account_id,
            sfdc_opportunity_xf.opportunity_category,


            case
                when sfdc_opportunity_xf.stage_1_date <= opp_snapshot.snapshot_date
                then sfdc_opportunity_xf.stage_1_date
                else null
            end as stage_1_date,

            case
                when sfdc_opportunity_xf.stage_1_date <= opp_snapshot.snapshot_date
                then sfdc_opportunity_xf.stage_1_date_month
                else null
            end as stage_1_date_month,

            case
                when sfdc_opportunity_xf.stage_1_date <= opp_snapshot.snapshot_date
                then sfdc_opportunity_xf.stage_1_fiscal_year
                else null
            end as stage_1_fiscal_year,

            case
                when sfdc_opportunity_xf.stage_1_date <= opp_snapshot.snapshot_date
                then sfdc_opportunity_xf.stage_1_fiscal_quarter_name
                else null
            end as stage_1_fiscal_quarter_name,

            case
                when sfdc_opportunity_xf.stage_1_date <= opp_snapshot.snapshot_date
                then sfdc_opportunity_xf.stage_1_fiscal_quarter_date
                else null
            end as stage_1_fiscal_quarter_date,

            -- ----------------------------------------------------------------------------------------------------
            -- ----------------------------------------------------------------------------------------------------
            -- DEPRECATED IACV METRICS
            -- Use Net ARR instead
            case
                when
                    opp_snapshot.pipeline_created_fiscal_quarter_name
                    = opp_snapshot.close_fiscal_quarter_name
                    and opp_snapshot.is_won = 1
                then opp_snapshot.incremental_acv
                else 0
            end as created_and_won_same_quarter_iacv,

            -- created within quarter
            case
                when
                    opp_snapshot.pipeline_created_fiscal_quarter_name
                    = opp_snapshot.snapshot_fiscal_quarter_name
                then opp_snapshot.incremental_acv
                else 0
            end as created_in_snapshot_quarter_iacv,

            -- field used for FY21 bookings reporitng
            sfdc_opportunity_xf.account_owner_team_stamped,

            -- temporary, to deal with global reports that use
            -- account_owner_team_stamp field
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

            -- Team Segment / ASM - RD 
            -- As the snapshot history table is used to compare current perspective
            -- with the past, I leverage the most recent version
            -- of the truth ato cut the data, that's why instead of using the stampped
            -- version, I take the current fields.
            -- https://gitlab.my.salesforce.com/00N6100000ICcrD?setupid=OpportunityFields
            /*

      FY23 - NF 2022-01-28 

      At this point I still think the best is to keep taking the owner / account demographics cuts from the most recent version of the opportunity object.

      The snapshot history at this point is mainly used to track how current performance compares with previous quarters and years
      and to do that effectively the patches / territories must be the same. Any error that is corrected in the future should be incorporated 
      into the overview

      */
            sfdc_opportunity_xf.opportunity_owner_user_segment,
            sfdc_opportunity_xf.opportunity_owner_user_region,
            sfdc_opportunity_xf.opportunity_owner_user_area,
            sfdc_opportunity_xf.opportunity_owner_user_geo,

            -- - target fields for reporting, changing their name might help to
            -- isolate their logic from the actual field
            -- -----------------
            -- NF 2022-01-28 TO BE DEPRECATED once pipeline velocity reports in
            -- Sisense are updated
            sfdc_opportunity_xf.sales_team_rd_asm_level,
            -- -----------------
            sfdc_opportunity_xf.sales_team_cro_level,
            sfdc_opportunity_xf.sales_team_vp_level,
            sfdc_opportunity_xf.sales_team_avp_rd_level,
            sfdc_opportunity_xf.sales_team_asm_level,

            -- this fields use the opportunity owner version for current FY and
            -- account fields for previous years
            sfdc_opportunity_xf.report_opportunity_user_segment,
            sfdc_opportunity_xf.report_opportunity_user_geo,
            sfdc_opportunity_xf.report_opportunity_user_region,
            sfdc_opportunity_xf.report_opportunity_user_area,

            -- NF 2022-02-17 new aggregated keys 
            sfdc_opportunity_xf.report_user_segment_geo_region_area,
            sfdc_opportunity_xf.report_user_segment_geo_region_area_sqs_ot,

            sfdc_opportunity_xf.key_sqs,
            sfdc_opportunity_xf.key_ot,

            sfdc_opportunity_xf.key_segment,
            sfdc_opportunity_xf.key_segment_sqs,
            sfdc_opportunity_xf.key_segment_ot,

            sfdc_opportunity_xf.key_segment_geo,
            sfdc_opportunity_xf.key_segment_geo_sqs,
            sfdc_opportunity_xf.key_segment_geo_ot,

            sfdc_opportunity_xf.key_segment_geo_region,
            sfdc_opportunity_xf.key_segment_geo_region_sqs,
            sfdc_opportunity_xf.key_segment_geo_region_ot,

            sfdc_opportunity_xf.key_segment_geo_region_area,
            sfdc_opportunity_xf.key_segment_geo_region_area_sqs,
            sfdc_opportunity_xf.key_segment_geo_region_area_ot,

            sfdc_opportunity_xf.key_segment_geo_area,

            -- using current opportunity perspective instead of historical
            -- NF 2021-01-26: this might change to order type live 2.1    
            -- NF 2022-01-28: Update to OT 2.3 will be stamped directly  
            sfdc_opportunity_xf.order_type_stamped,

            -- top level grouping of the order type field
            sfdc_opportunity_xf.deal_group,

            -- medium level grouping of the order type field
            sfdc_opportunity_xf.deal_category,

            -- duplicates flag
            sfdc_opportunity_xf.is_duplicate_flag as current_is_duplicate_flag,

            -- the owner name in the opportunity is not clean.
            opportunity_owner.name as opportunity_owner,

            -- ----------------------------------------------------------------------------------------------------
            -- ----------------------------------------------------------------------------------------------------
            -- account driven fields
            sfdc_accounts_xf.account_name,
            sfdc_accounts_xf.tsp_region,
            sfdc_accounts_xf.tsp_sub_region,
            sfdc_accounts_xf.ultimate_parent_sales_segment,
            sfdc_accounts_xf.tsp_max_hierarchy_sales_segment,
            sfdc_accounts_xf.ultimate_parent_account_id,
            upa.account_name as ultimate_parent_account_name,
            sfdc_accounts_xf.ultimate_parent_id,
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
            as upa_demographics_territory


        from sfdc_opportunity_snapshot_history opp_snapshot
        inner join
            sfdc_opportunity_xf
            on sfdc_opportunity_xf.opportunity_id = opp_snapshot.opportunity_id
        left join
            sfdc_accounts_xf
            on sfdc_opportunity_xf.account_id = sfdc_accounts_xf.account_id
        left join
            sfdc_accounts_xf upa
            on upa.account_id = sfdc_accounts_xf.ultimate_parent_account_id
        left join
            sfdc_users_xf account_owner
            on account_owner.user_id = sfdc_accounts_xf.owner_id
        left join
            sfdc_users_xf opportunity_owner
            on opportunity_owner.user_id = opp_snapshot.owner_id
        -- Net IACV to Net ARR conversion table
        left join
            net_iacv_to_net_arr_ratio
            on net_iacv_to_net_arr_ratio.user_segment_stamped
            = sfdc_opportunity_xf.opportunity_owner_user_segment
            and net_iacv_to_net_arr_ratio.order_type_stamped
            = sfdc_opportunity_xf.order_type_stamped
        where  -- remove test account
            opp_snapshot.raw_account_id not in ('0014M00001kGcORQA0')
            and (
                sfdc_accounts_xf.ultimate_parent_account_id
                not in ('0016100001YUkWVAA1')
                or sfdc_accounts_xf.account_id is null  -- remove test account
            )
            and opp_snapshot.is_deleted = 0
            -- NF 20210906 remove JiHu opties from the models
            and sfdc_accounts_xf.is_jihu_account = 0

    -- in Q2 FY21 a few deals where created in the wrong stage, and as they were
    -- purely aspirational,
    -- they needed to be removed from stage 1, eventually by the end of the quarter
    -- they were removed
    -- The goal of this list is to use in the Created Pipeline flag, to exclude those
    -- deals that at
    -- day 90 had stages of less than 1, that should smooth the chart
    ),
    vision_opps as (

        select
            opp_snapshot.opportunity_id,
            opp_snapshot.stage_name,
            opp_snapshot.snapshot_fiscal_quarter_date
        from sfdc_opportunity_snapshot_history_xf opp_snapshot
        where
            opp_snapshot.snapshot_fiscal_quarter_name = 'FY21-Q2'
            and opp_snapshot.pipeline_created_fiscal_quarter_date
            = opp_snapshot.snapshot_fiscal_quarter_date
            and opp_snapshot.snapshot_day_of_fiscal_quarter_normalised = 90
            and opp_snapshot.stage_name
            in ('00-Pre Opportunity', '0-Pending Acceptance', '0-Qualifying')
        group by 1, 2, 3


    ),
    add_compound_metrics as (

        select
            opp_snapshot.*,

            -- ----------------------------
            -- compound metrics for reporting
            -- ----------------------------
            -- current deal size field, it was creasted by the data team and the
            -- original doesn't work
            case
                when opp_snapshot.net_arr > 0 and net_arr < 5000
                then '1 - Small (<5k)'
                when opp_snapshot.net_arr >= 5000 and net_arr < 25000
                then '2 - Medium (5k - 25k)'
                when opp_snapshot.net_arr >= 25000 and net_arr < 100000
                then '3 - Big (25k - 100k)'
                when opp_snapshot.net_arr >= 100000
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

            -- Open pipeline eligibility definition
            case
                when
                    lower(opp_snapshot.deal_group) like any ('%growth%', '%new%')
                    and opp_snapshot.is_edu_oss = 0
                    and opp_snapshot.is_stage_1_plus = 1
                    and opp_snapshot.forecast_category_name != 'Omitted'
                    and opp_snapshot.is_open = 1
                then 1
                else 0
            end as is_eligible_open_pipeline_flag,


            -- Created pipeline eligibility definition
            -- https://gitlab.com/gitlab-com/sales-team/field-operations/systems/-/issues/2389
            case
                when
                    opp_snapshot.order_type_stamped
                    in ('1. New - First Order', '2. New - Connected', '3. Growth')
                    and opp_snapshot.is_edu_oss = 0
                    and opp_snapshot.pipeline_created_fiscal_quarter_date is not null
                    and opp_snapshot.opportunity_category in (
                        'Standard',
                        'Internal Correction',
                        'Ramp Deal',
                        'Credit',
                        'Contract Reset'
                    )
                    and opp_snapshot.stage_name not in (
                        '00-Pre Opportunity',
                        '10-Duplicate',
                        '9-Unqualified',
                        '0-Pending Acceptance'
                    )
                    and (
                        opp_snapshot.net_arr > 0
                        or opp_snapshot.opportunity_category = 'Credit'
                    )
                    -- exclude vision opps from FY21-Q2
                    and (
                        opp_snapshot.pipeline_created_fiscal_quarter_name != 'FY21-Q2'
                        or vision_opps.opportunity_id is null
                    )
                    -- 20220128 Updated to remove webdirect SQS deals 
                    and opp_snapshot.sales_qualified_source != 'Web Direct Generated'
                then 1
                else 0
            end as is_eligible_created_pipeline_flag,

            -- SAO alignment issue:
            -- https://gitlab.com/gitlab-com/sales-team/field-operations/sales-operations/-/issues/2656
            case
                when
                    opp_snapshot.sales_accepted_date is not null
                    and opp_snapshot.is_edu_oss = 0
                    and opp_snapshot.is_deleted = 0
                    and opp_snapshot.is_renewal = 0
                    and opp_snapshot.stage_name
                    not in ('10-Duplicate', '9-Unqualified', '0-Pending Acceptance')
                then 1
                else 0
            end as is_eligible_sao_flag,

            -- ASP Analysis eligibility issue:
            -- https://gitlab.com/gitlab-com/sales-team/field-operations/sales-operations/-/issues/2606
            case
                when
                    opp_snapshot.is_edu_oss = 0
                    and opp_snapshot.is_deleted = 0
                    -- For ASP we care mainly about add on, new business, excluding
                    -- contraction / churn
                    and opp_snapshot.order_type_stamped
                    in ('1. New - First Order', '2. New - Connected', '3. Growth')
                    -- Exclude Decomissioned as they are not aligned to the real owner
                    -- Contract Reset, Decomission
                    and opp_snapshot.opportunity_category
                    in ('Standard', 'Ramp Deal', 'Internal Correction')
                    -- Exclude Deals with nARR < 0
                    and net_arr > 0
                -- Not JiHu
                then 1
                else 0
            end as is_eligible_asp_analysis_flag,

            -- Age eligibility issue:
            -- https://gitlab.com/gitlab-com/sales-team/field-operations/sales-operations/-/issues/2606
            case
                when
                    opp_snapshot.is_edu_oss = 0
                    and opp_snapshot.is_deleted = 0
                    -- Renewals are not having the same motion as rest of deals
                    and opp_snapshot.is_renewal = 0
                    -- For stage age we exclude only ps/other
                    and opp_snapshot.order_type_stamped in (
                        '1. New - First Order',
                        '2. New - Connected',
                        '3. Growth',
                        '4. Contraction',
                        '6. Churn - Final',
                        '5. Churn - Partial'
                    )
                    -- Only include deal types with meaningful journeys through the
                    -- stages
                    and opp_snapshot.opportunity_category
                    in ('Standard', 'Ramp Deal', 'Decommissioned')
                    -- Web Purchase have a different dynamic and should not be included
                    and opp_snapshot.is_web_portal_purchase = 0
                -- Not JiHu
                then 1
                else 0
            end as is_eligible_age_analysis_flag,

            -- TODO: This is the same as FP&A Boookings Flag
            case
                when
                    opp_snapshot.is_edu_oss = 0
                    and opp_snapshot.is_deleted = 0
                    and (
                        opp_snapshot.is_won = 1
                        or (opp_snapshot.is_renewal = 1 and opp_snapshot.is_lost = 1)
                    )
                    and opp_snapshot.order_type_stamped in (
                        '1. New - First Order',
                        '2. New - Connected',
                        '3. Growth',
                        '4. Contraction',
                        '6. Churn - Final',
                        '5. Churn - Partial'
                    )
                -- Not JiHu
                then 1
                else 0
            end as is_booked_net_arr_flag,

            case
                when
                    opp_snapshot.is_edu_oss = 0
                    and opp_snapshot.is_deleted = 0
                    and opp_snapshot.order_type_stamped
                    in ('4. Contraction', '6. Churn - Final', '5. Churn - Partial')
                -- Not JiHu
                then 1
                else 0
            end as is_eligible_churn_contraction_flag,

            -- created within quarter
            case
                when
                    opp_snapshot.pipeline_created_fiscal_quarter_name
                    = opp_snapshot.snapshot_fiscal_quarter_name
                    and is_eligible_created_pipeline_flag = 1
                then opp_snapshot.net_arr
                else 0
            end as created_in_snapshot_quarter_net_arr,

            -- created and closed within the quarter net arr
            case
                when
                    opp_snapshot.pipeline_created_fiscal_quarter_name
                    = opp_snapshot.close_fiscal_quarter_name
                    and is_won = 1
                    and is_eligible_created_pipeline_flag = 1
                then opp_snapshot.net_arr
                else 0
            end as created_and_won_same_quarter_net_arr,


            case
                when
                    opp_snapshot.pipeline_created_fiscal_quarter_name
                    = opp_snapshot.snapshot_fiscal_quarter_name
                    and is_eligible_created_pipeline_flag = 1
                then opp_snapshot.calculated_deal_count
                else 0
            end as created_in_snapshot_quarter_deal_count,

            -- -------------------------------------------------------------------------------------------------------
            -- -------------------------------------------------------------------------------------------------------
            -- Fields created to simplify report building down the road. Specially the
            -- pipeline velocity.
            -- deal count
            case
                when
                    is_eligible_open_pipeline_flag = 1
                    and opp_snapshot.is_stage_1_plus = 1
                then opp_snapshot.calculated_deal_count
                else 0
            end as open_1plus_deal_count,

            case
                when
                    is_eligible_open_pipeline_flag = 1
                    and opp_snapshot.is_stage_3_plus = 1
                then opp_snapshot.calculated_deal_count
                else 0
            end as open_3plus_deal_count,

            case
                when
                    is_eligible_open_pipeline_flag = 1
                    and opp_snapshot.is_stage_4_plus = 1
                then opp_snapshot.calculated_deal_count
                else 0
            end as open_4plus_deal_count,

            -- booked deal count
            case
                when opp_snapshot.is_won = 1
                then opp_snapshot.calculated_deal_count
                else 0
            end as booked_deal_count,

            -- churned contraction deal count as OT
            case
                when
                    (
                        (opp_snapshot.is_renewal = 1 and opp_snapshot.is_lost = 1)
                        or opp_snapshot.is_won = 1
                    )
                    and opp_snapshot.order_type_stamped
                    in ('5. Churn - Partial', '6. Churn - Final', '4. Contraction')
                then opp_snapshot.calculated_deal_count
                else 0
            end as churned_contraction_deal_count,

            -- ---------------
            -- Net ARR
            case
                when is_eligible_open_pipeline_flag = 1 then opp_snapshot.net_arr else 0
            end as open_1plus_net_arr,

            case
                when
                    is_eligible_open_pipeline_flag = 1
                    and opp_snapshot.is_stage_3_plus = 1
                then opp_snapshot.net_arr
                else 0
            end as open_3plus_net_arr,

            case
                when
                    is_eligible_open_pipeline_flag = 1
                    and opp_snapshot.is_stage_4_plus = 1
                then opp_snapshot.net_arr
                else 0
            end as open_4plus_net_arr,

            -- booked net arr (won + renewals / lost)
            case
                when
                    (
                        opp_snapshot.is_won = 1
                        or (opp_snapshot.is_renewal = 1 and opp_snapshot.is_lost = 1)
                    )
                then opp_snapshot.net_arr
                else 0
            end as booked_net_arr,

            -- churned contraction deal count as OT
            case
                when
                    (
                        (opp_snapshot.is_renewal = 1 and opp_snapshot.is_lost = 1)
                        or opp_snapshot.is_won = 1
                    )
                    and opp_snapshot.order_type_stamped
                    in ('5. Churn - Partial', '6. Churn - Final', '4. Contraction')
                then net_arr
                else 0
            end as churned_contraction_net_arr,

            -- 20201021 NF: This should be replaced by a table that keeps track of
            -- excluded deals for forecasting purposes
            case
                when
                    opp_snapshot.ultimate_parent_id in (
                        '001610000111bA3',
                        '0016100001F4xla',
                        '0016100001CXGCs',
                        '00161000015O9Yn',
                        '0016100001b9Jsc'
                    )
                    and opp_snapshot.close_date < '2020-08-01'
                then 1
                -- NF 2021 - Pubsec extreme deals
                when
                    opp_snapshot.opportunity_id
                    in ('0064M00000WtZKUQA3', '0064M00000Xb975QAB')
                    and opp_snapshot.snapshot_date < '2021-05-01'
                then 1
                -- exclude vision opps from FY21-Q2
                when
                    opp_snapshot.pipeline_created_fiscal_quarter_name = 'FY21-Q2'
                    and vision_opps.opportunity_id is not null
                then 1
                -- NF 20220415 PubSec duplicated deals on Pipe Gen -- Lockheed Martin
                -- GV - 40000 Ultimate Renewal
                when
                    opp_snapshot.opportunity_id in (
                        '0064M00000ZGpfQQAT', '0064M00000ZGpfVQAT', '0064M00000ZGpfGQAT'
                    )
                then 1

                else 0
            end as is_excluded_flag


        from sfdc_opportunity_snapshot_history_xf opp_snapshot
        left join
            vision_opps
            on vision_opps.opportunity_id = opp_snapshot.opportunity_id
            and vision_opps.snapshot_fiscal_quarter_date
            = opp_snapshot.snapshot_fiscal_quarter_date

    )

select *
from add_compound_metrics

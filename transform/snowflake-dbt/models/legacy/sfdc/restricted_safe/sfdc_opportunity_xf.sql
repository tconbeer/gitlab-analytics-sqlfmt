with
    sfdc_opportunity as (select * from {{ ref("sfdc_opportunity") }}),
    sfdc_opportunity_stage as (select * from {{ ref("sfdc_opportunity_stage") }}),
    sfdc_lead_source as (select * from {{ ref("sfdc_lead_sources") }}),
    sfdc_users_xf as (select * from {{ ref("sfdc_users_xf") }}),
    sfdc_record_type as (select * from {{ ref("sfdc_record_type") }}),
    sfdc_account as (select * from {{ ref("sfdc_account") }}),
    date_details as (

        select
            *,
            dense_rank() over (order by first_day_of_fiscal_quarter) as quarter_number
        from {{ ref("date_details") }}
        order by 1 desc

    ),
    sales_admin_hierarchy as (

        select
            sfdc_opportunity.opportunity_id,
            sfdc_opportunity.owner_id,
            'CRO' as level_1,
            case
                account_owner_team_stamped
                when 'APAC'
                then 'VP Ent'
                when 'Commercial'
                then 'VP Comm SMB'
                when 'Commercial - MM'
                then 'VP Comm MM'
                when 'Commercial - SMB'
                then 'VP Comm SMB'
                when 'EMEA'
                then 'VP Ent'
                when 'MM - APAC'
                then 'VP Comm MM'
                when 'MM - East'
                then 'VP Comm MM'
                when 'MM - EMEA'
                then 'VP Comm MM'
                when 'MM - West'
                then 'VP Comm MM'
                when 'MM-EMEA'
                then 'VP Comm MM'
                when 'Public Sector'
                then 'VP Ent'
                when 'SMB'
                then 'VP Comm SMB'
                when 'SMB - International'
                then 'VP Comm SMB'
                when 'SMB - US'
                then 'VP Comm SMB'
                when 'US East'
                then 'VP Ent'
                when 'US West'
                then 'VP Ent'
                else null
            end as level_2,
            case
                account_owner_team_stamped
                when 'APAC'
                then 'RD APAC'
                when 'EMEA'
                then 'RD EMEA'
                when 'MM - APAC'
                then 'ASM - MM - APAC'
                when 'MM - East'
                then 'ASM - MM - East'
                when 'MM - EMEA'
                then 'ASM - MM - EMEA'
                when 'MM - West'
                then 'ASM - MM - West'
                when 'MM-EMEA'
                then 'ASM - MM - EMEA'
                when 'Public Sector'
                then 'RD PubSec'
                when 'US East'
                then 'RD US East'
                when 'US West'
                then 'RD US West'
                else null
            end as level_3
        from sfdc_opportunity
        -- sfdc Sales Admin user
        where owner_id = '00561000000mpHTAAY'

    ),
    layered as (

        select
            -- keys
            sfdc_opportunity.account_id,
            sfdc_opportunity.opportunity_id,
            sfdc_opportunity.opportunity_name,
            sfdc_opportunity.owner_id,

            -- logistical information
            sfdc_opportunity.close_date,
            sfdc_opportunity.created_date,
            sfdc_opportunity.days_in_stage,
            sfdc_opportunity.deployment_preference,
            sfdc_opportunity.generated_source,
            sfdc_opportunity.lead_source,
            sfdc_lead_source.lead_source_id as lead_source_id,
            coalesce(sfdc_lead_source.initial_source, 'Unknown') as lead_source_name,
            coalesce(
                sfdc_lead_source.initial_source_type, 'Unknown'
            ) as lead_source_type,
            sfdc_opportunity.merged_opportunity_id,
            sfdc_opportunity.net_new_source_categories,
            sfdc_opportunity.opportunity_business_development_representative,
            sfdc_opportunity.opportunity_owner as opportunity_owner,
            sfdc_opportunity.opportunity_owner_department
            as opportunity_owner_department,
            sfdc_opportunity.opportunity_owner_manager as opportunity_owner_manager,
            opportunity_owner.role_name as opportunity_owner_role,
            opportunity_owner.title as opportunity_owner_title,
            sfdc_opportunity.opportunity_sales_development_representative,
            sfdc_opportunity.opportunity_development_representative,
            sfdc_opportunity.account_owner_team_stamped,
            sfdc_opportunity.opportunity_term,
            sfdc_opportunity.primary_campaign_source_id as primary_campaign_source_id,
            sfdc_opportunity.sales_accepted_date,
            sfdc_opportunity.sales_path,
            sfdc_opportunity.sales_qualified_date,
            sfdc_opportunity.sales_type,
            sfdc_opportunity.sdr_pipeline_contribution,
            sfdc_opportunity.source_buckets,
            sfdc_opportunity.stage_name,
            sfdc_opportunity_stage.is_active as stage_is_active,
            sfdc_opportunity_stage.is_closed as stage_is_closed,
            sfdc_opportunity.technical_evaluation_date,
            sfdc_opportunity.order_type,
            sfdc_opportunity.deal_path,
            sfdc_opportunity.opportunity_category,

            -- opportunity information
            sfdc_opportunity.acv,
            sfdc_opportunity.amount,
            sfdc_opportunity.closed_deals,
            sfdc_opportunity.competitors,
            sfdc_opportunity.critical_deal_flag,
            sfdc_opportunity.deal_size,
            sfdc_opportunity.forecast_category_name,
            sfdc_opportunity.forecasted_iacv,
            sfdc_opportunity.iacv_created_date,
            sfdc_opportunity.incremental_acv,
            sfdc_opportunity.pre_covid_iacv,
            sfdc_opportunity.invoice_number,
            sfdc_opportunity.is_refund,
            sfdc_opportunity.is_downgrade,
            case
                when
                    (
                        sfdc_opportunity.days_in_stage > 30
                        or sfdc_opportunity.incremental_acv > 100000
                        or sfdc_opportunity.pushed_count > 0
                    )
                then true
                else false
            end as is_risky,
            sfdc_opportunity.is_swing_deal,
            sfdc_opportunity.is_edu_oss,
            sfdc_opportunity_stage.is_won as is_won,
            sfdc_opportunity.net_incremental_acv,
            sfdc_opportunity.probability,
            sfdc_opportunity.professional_services_value,
            sfdc_opportunity.pushed_count,
            sfdc_opportunity.reason_for_loss,
            sfdc_opportunity.reason_for_loss_details,
            sfdc_opportunity.downgrade_reason,
            sfdc_opportunity.refund_iacv,
            sfdc_opportunity.downgrade_iacv,
            sfdc_opportunity.renewal_acv,
            sfdc_opportunity.renewal_amount,
            sfdc_opportunity.sales_qualified_source,
            sfdc_opportunity.solutions_to_be_replaced,
            sfdc_opportunity.total_contract_value,
            sfdc_opportunity.upside_iacv,
            sfdc_opportunity.upside_swing_deal_iacv,
            sfdc_opportunity.incremental_acv * (probability / 100) as weighted_iacv,
            sfdc_opportunity.is_web_portal_purchase,
            sfdc_opportunity.partner_initiated_opportunity,
            sfdc_opportunity.user_segment,
            sfdc_opportunity.subscription_start_date,
            sfdc_opportunity.subscription_end_date,
            sfdc_opportunity.true_up_value,
            sfdc_opportunity.order_type_live,
            sfdc_opportunity.order_type_stamped,
            sfdc_opportunity.net_arr,
            sfdc_opportunity.recurring_amount,
            sfdc_opportunity.true_up_amount,
            sfdc_opportunity.proserv_amount,
            sfdc_opportunity.other_non_recurring_amount,
            sfdc_opportunity.arr_basis,
            sfdc_opportunity.arr,
            sfdc_opportunity.opportunity_health,
            sfdc_opportunity.risk_type,
            sfdc_opportunity.risk_reasons,
            sfdc_opportunity.tam_notes,
            sfdc_opportunity.primary_solution_architect,
            sfdc_opportunity.product_details,
            sfdc_opportunity.product_category,
            sfdc_opportunity.products_purchased,

            -- days and dates per stage
            sfdc_opportunity.days_in_1_discovery,
            sfdc_opportunity.days_in_2_scoping,
            sfdc_opportunity.days_in_3_technical_evaluation,
            sfdc_opportunity.days_in_4_proposal,
            sfdc_opportunity.days_in_5_negotiating,
            sfdc_opportunity.stage_0_pending_acceptance_date,
            sfdc_opportunity.stage_1_discovery_date,
            sfdc_opportunity.stage_2_scoping_date,
            sfdc_opportunity.stage_3_technical_evaluation_date,
            sfdc_opportunity.stage_4_proposal_date,
            sfdc_opportunity.stage_5_negotiating_date,
            sfdc_opportunity.stage_6_awaiting_signature_date,
            sfdc_opportunity.stage_6_closed_won_date,
            sfdc_opportunity.stage_6_closed_lost_date,

            -- helper flag, tracks won deals & renewals + not jihu
            sfdc_opportunity.fpa_master_bookings_flag,

            -- command plan fields
            sfdc_opportunity.cp_champion,
            sfdc_opportunity.cp_close_plan,
            sfdc_opportunity.cp_competition,
            sfdc_opportunity.cp_decision_criteria,
            sfdc_opportunity.cp_decision_process,
            sfdc_opportunity.cp_economic_buyer,
            sfdc_opportunity.cp_identify_pain,
            sfdc_opportunity.cp_metrics,
            sfdc_opportunity.cp_risks,
            sfdc_opportunity.cp_use_cases,
            sfdc_opportunity.cp_value_driver,
            sfdc_opportunity.cp_why_do_anything_at_all,
            sfdc_opportunity.cp_why_gitlab,
            sfdc_opportunity.cp_why_now,

            -- User Segment Hierarchy fields
            sfdc_opportunity.user_segment_stamped,
            sfdc_opportunity.user_geo_stamped,
            sfdc_opportunity.user_region_stamped,
            sfdc_opportunity.user_area_stamped,

            -- sales segment refactor
            sfdc_opportunity.division_sales_segment_stamped,
            {{ sales_segment_cleaning("sfdc_account.tsp_max_hierarchy_sales_segment") }}
            as tsp_max_hierarchy_sales_segment,
            sfdc_account.division_sales_segment,
            sfdc_account.ultimate_parent_sales_segment,
            sfdc_account.is_jihu_account,
            sfdc_account.gitlab_partner_program,

            -- ************************************
            -- sales segmentation deprecated fields - 2020-09-03
            -- left temporary for the sake of MVC and avoid breaking SiSense existing
            -- charts
            -- issue: https://gitlab.com/gitlab-data/analytics/-/issues/5709
            sfdc_opportunity.sales_segment as sales_segment,
            sfdc_opportunity.parent_segment as parent_segment,

            -- ************************************
            -- channel reporting
            -- issue: https://gitlab.com/gitlab-data/analytics/-/issues/6072
            sfdc_opportunity.dr_partner_deal_type,
            sfdc_opportunity.dr_partner_engagement,
            sfdc_opportunity.partner_account,
            sfdc_opportunity.dr_status,
            sfdc_opportunity.distributor,
            sfdc_opportunity.influence_partner,
            sfdc_opportunity.fulfillment_partner,
            sfdc_opportunity.platform_partner,
            sfdc_opportunity.partner_track,
            sfdc_opportunity.is_public_sector_opp,
            sfdc_opportunity.is_registration_from_portal,
            sfdc_opportunity.calculated_discount,
            sfdc_opportunity.partner_discount,
            sfdc_opportunity.partner_discount_calc,
            sfdc_opportunity.comp_channel_neutral,

            -- account owner hierarchies levels
            account_owner.sales_team_level_2 as account_owner_team_level_2,
            account_owner.sales_team_level_3 as account_owner_team_level_3,
            account_owner.sales_team_level_4 as account_owner_team_level_4,
            account_owner.sales_team_vp_level as account_owner_team_vp_level,
            account_owner.sales_team_rd_level as account_owner_team_rd_level,
            account_owner.sales_team_asm_level as account_owner_team_asm_level,
            account_owner.sales_min_hierarchy_level as account_owner_min_team_level,
            account_owner.sales_region as account_owner_sales_region,

            -- opportunity owner hierarchies levels
            case
                when sales_admin_hierarchy.level_2 is not null
                then sales_admin_hierarchy.level_2
                else opportunity_owner.sales_team_level_2
            end as opportunity_owner_team_level_2,
            case
                when sales_admin_hierarchy.level_3 is not null
                then sales_admin_hierarchy.level_3
                else opportunity_owner.sales_team_level_3
            end as opportunity_owner_team_level_3,

            -- reporting helper flags
            case
                when
                    sfdc_opportunity.stage_name in (
                        '0-Pending Acceptance',
                        '0-Qualifying',
                        'Developing',
                        '1-Discovery',
                        '2-Developing',
                        '2-Scoping'
                    )
                then 'Pipeline'
                when
                    sfdc_opportunity.stage_name in (
                        '3-Technical Evaluation',
                        '4-Proposal',
                        '5-Negotiating',
                        '6-Awaiting Signature',
                        '7-Closing'
                    )
                then '3+ Pipeline'
                when sfdc_opportunity.stage_name in ('8-Closed Lost', 'Closed Lost')
                then 'Lost'
                when sfdc_opportunity.stage_name in ('Closed Won')
                then 'Closed Won'
                else 'Other'
            end as stage_name_3plus,

            case
                when
                    sfdc_opportunity.stage_name in (
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
                    sfdc_opportunity.stage_name in (
                        '4-Proposal',
                        '5-Negotiating',
                        '6-Awaiting Signature',
                        '7-Closing'
                    )
                then '4+ Pipeline'
                when sfdc_opportunity.stage_name in ('8-Closed Lost', 'Closed Lost')
                then 'Lost'
                when sfdc_opportunity.stage_name in ('Closed Won')
                then 'Closed Won'
                else 'Other'
            end as stage_name_4plus,

            case
                when
                    sfdc_opportunity.stage_name in (
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
                when sfdc_opportunity.stage_name = '8-Closed Lost' then 1 else 0
            end as is_lost,
            case
                when
                    (
                        sfdc_opportunity.stage_name = '8-Closed Lost'
                        or sfdc_opportunity.stage_name = '9-Unqualified'
                        or sfdc_opportunity_stage.is_won = 1
                    )
                then 0
                else 1
            end as is_open,

            case when is_open = 0 then 1 else 0 end as is_closed,

            case
                when sfdc_opportunity_stage.is_won = 1
                then '1.Won'
                when is_lost = 1
                then '2.Lost'
                when is_open = 1
                then '0. Open'
                else 'N/A'
            end as stage_category,

            case
                when lower(sfdc_opportunity.sales_type) like '%renewal%' then 1 else 0
            end as is_renewal,

            -- date fields helpers
            close_date_detail.fiscal_quarter_name_fy as close_fiscal_quarter_name,
            close_date_detail.first_day_of_fiscal_quarter as close_fiscal_quarter_date,
            close_date_detail.fiscal_year as close_fiscal_year,
            close_date_detail.first_day_of_month as close_date_month,

            created_date_detail.fiscal_quarter_name_fy as created_fiscal_quarter_name,
            created_date_detail.first_day_of_fiscal_quarter
            as created_fiscal_quarter_date,
            created_date_detail.fiscal_year as created_fiscal_year,
            created_date_detail.first_day_of_month as created_date_month,

            start_date.fiscal_quarter_name_fy
            as subscription_start_date_fiscal_quarter_name,
            start_date.first_day_of_fiscal_quarter
            as subscription_start_date_fiscal_quarter_date,
            start_date.fiscal_year as subscription_start_date_fiscal_year,
            start_date.first_day_of_month as subscription_start_date_month,

            sales_accepted_date.fiscal_quarter_name_fy
            as sales_accepted_fiscal_quarter_name,
            sales_accepted_date.first_day_of_fiscal_quarter
            as sales_accepted_fiscal_quarter_date,
            sales_accepted_date.fiscal_year as sales_accepted_fiscal_year,
            sales_accepted_date.first_day_of_month as sales_accepted_date_month,

            sales_qualified_date.fiscal_quarter_name_fy
            as sales_qualified_fiscal_quarter_name,
            sales_qualified_date.first_day_of_fiscal_quarter
            as sales_qualified_fiscal_quarter_date,
            sales_qualified_date.fiscal_year as sales_qualified_fiscal_year,
            sales_qualified_date.first_day_of_month as sales_qualified_date_month,

            iacv_created_date.fiscal_quarter_name_fy
            as iacv_created_fiscal_quarter_name,
            iacv_created_date.first_day_of_fiscal_quarter
            as iacv_created_fiscal_quarter_date,
            iacv_created_date.fiscal_year as iacv_created_fiscal_year,
            iacv_created_date.first_day_of_month as iacv_created_date_month,

            -- metadata
            sfdc_opportunity._last_dbt_run,
            sfdc_record_type.business_process_id,
            sfdc_opportunity.days_since_last_activity,
            sfdc_opportunity.is_deleted,
            sfdc_opportunity.last_activity_date,
            sfdc_record_type.record_type_description,
            sfdc_opportunity.record_type_id,
            sfdc_record_type.record_type_label,
            sfdc_record_type.record_type_modifying_object_type,
            sfdc_record_type.record_type_name,
            md5(
                (date_trunc('month', sfdc_opportunity.close_date)::date) || upper(
                    opportunity_owner.team
                )
            ) as region_quota_id,
            md5(
                (date_trunc('month', sfdc_opportunity.close_date)::date) || upper(
                    opportunity_owner.name
                )
            ) as sales_quota_id

        from sfdc_opportunity
        inner join
            sfdc_opportunity_stage
            on sfdc_opportunity.stage_name = sfdc_opportunity_stage.primary_label
        inner join
            date_details close_date_detail
            on close_date_detail.date_actual = sfdc_opportunity.close_date::date
        inner join
            date_details created_date_detail
            on created_date_detail.date_actual = sfdc_opportunity.created_date::date
        left join
            sfdc_lead_source
            on sfdc_opportunity.lead_source = sfdc_lead_source.initial_source
        left join
            sfdc_users_xf opportunity_owner
            on sfdc_opportunity.owner_id = opportunity_owner.user_id
        left join
            sfdc_record_type
            on sfdc_opportunity.record_type_id = sfdc_record_type.record_type_id
        left join sfdc_account on sfdc_account.account_id = sfdc_opportunity.account_id
        left join
            date_details sales_accepted_date
            on sfdc_opportunity.sales_accepted_date::date
            = sales_accepted_date.date_actual
        left join
            date_details start_date
            on sfdc_opportunity.subscription_start_date::date = start_date.date_actual
        left join
            date_details sales_qualified_date
            on sfdc_opportunity.sales_qualified_date::date
            = sales_qualified_date.date_actual
        left join
            date_details iacv_created_date
            on iacv_created_date.date_actual = sfdc_opportunity.iacv_created_date::date
        left join
            sfdc_users_xf account_owner on account_owner.user_id = sfdc_account.owner_id
        left join
            sales_admin_hierarchy
            on sfdc_opportunity.opportunity_id = sales_admin_hierarchy.opportunity_id
    )

select *
from layered

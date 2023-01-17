{{ config(tags=["mnpi"]) }}

with
    source as (

        select
            opportunity.*,
            case
                when stagename = '0-Pending Acceptance'
                then x0_pending_acceptance_date__c
                when stagename = '1-Discovery'
                then x1_discovery_date__c
                when stagename = '2-Scoping'
                then x2_scoping_date__c
                when stagename = '3-Technical Evaluation'
                then x3_technical_evaluation_date__c
                when stagename = '4-Proposal'
                then x4_proposal_date__c
                when stagename = '5-Negotiating'
                then x5_negotiating_date__c
                when stagename = '6-Awaiting Signature'
                then x6_awaiting_signature_date__c
            end as calculation_days_in_stage_date,
            datediff(days, calculation_days_in_stage_date::date, current_date::date)
            + 1 as days_in_stage
        from {{ source("salesforce", "opportunity") }} as opportunity

    ),
    renamed as (

        select
            -- keys
            accountid as account_id,
            id as opportunity_id,
            name as opportunity_name,
            ownerid as owner_id,

            -- logistical information
            isclosed as is_closed,
            iswon as is_won,
            closedate as close_date,
            createddate as created_date,
            days_in_stage as days_in_stage,
            deployment_preference__c as deployment_preference,
            sql_source__c as generated_source,
            leadsource as lead_source,
            merged_opportunity__c as merged_opportunity_id,
            duplicate_opportunity__c as duplicate_opportunity_id,
            account_owner__c as account_owner,
            opportunity_owner__c as opportunity_owner,
            manager_current__c as opportunity_owner_manager,
            sales_market__c as opportunity_owner_department,
            sdr_lu__c as opportunity_sales_development_representative,
            business_development_representative__c
            as opportunity_business_development_representative,
            bdr_lu__c as opportunity_business_development_representative_lookup,
            bdr_sdr__c as opportunity_development_representative,

            account_owner_team_o__c as account_owner_team_stamped,

            sales_accepted_date__c as sales_accepted_date,
            engagement_type__c as sales_path,
            sales_qualified_date__c as sales_qualified_date,
            iqm_submitted_by_role__c as iqm_submitted_by_role,

            type as sales_type,
            {{ sfdc_source_buckets("leadsource") }}
            stagename as stage_name,
            revenue_type__c as order_type,
            deal_path__c as deal_path,

            -- opportunity information
            acv_2__c as acv,
            amount as amount,
            -- so that you can exclude closed deals that had negative impact
            iff(acv_2__c >= 0, 1, 0) as closed_deals,
            competitors__c as competitors,
            critical_deal_flag__c as critical_deal_flag,
            {{ sfdc_deal_size("incremental_acv_2__c", "deal_size") }},
            forecastcategoryname as forecast_category_name,
            incremental_acv_2__c as forecasted_iacv,
            iacv_created_date__c as iacv_created_date,
            incremental_acv__c as incremental_acv,
            pre_covid_iacv__c as pre_covid_iacv,
            invoice_number__c as invoice_number,
            is_refund_opportunity__c as is_refund,
            is_downgrade_opportunity__c as is_downgrade,
            swing_deal__c as is_swing_deal,
            is_edu_oss_opportunity__c as is_edu_oss,
            is_ps_opportunity__c as is_ps_opp,
            net_iacv__c as net_incremental_acv,
            campaignid as primary_campaign_source_id,
            probability as probability,
            professional_services_value__c as professional_services_value,
            push_counter__c as pushed_count,
            reason_for_lost__c as reason_for_loss,
            reason_for_lost_details__c as reason_for_loss_details,
            refund_iacv__c as refund_iacv,
            downgrade_iacv__c as downgrade_iacv,
            renewal_acv__c as renewal_acv,
            renewal_amount__c as renewal_amount,
            {{ sales_qualified_source_cleaning("sql_source__c") }}
            as sales_qualified_source,
            case
                when sales_qualified_source = 'BDR Generated'
                then 'SDR Generated'
                when
                    sales_qualified_source like any ('Web%', 'Missing%', 'Other')
                    or sales_qualified_source is null
                then 'Web Direct Generated'
                else sales_qualified_source
            end as sales_qualified_source_grouped,
            iff(
                sales_qualified_source = 'Channel Generated',
                'Partner Sourced',
                'Co-sell'
            ) as sqs_bucket_engagement,
            sdr_pipeline_contribution__c as sdr_pipeline_contribution,
            solutions_to_be_replaced__c as solutions_to_be_replaced,
            x3_technical_evaluation_date__c as technical_evaluation_date,
            amount as total_contract_value,
            recurring_amount__c as recurring_amount,
            true_up_amount__c as true_up_amount,
            proserv_amount__c as proserv_amount,
            other_non_recurring_amount__c as other_non_recurring_amount,
            upside_iacv__c as upside_iacv,
            upside_swing_deal_iacv__c as upside_swing_deal_iacv,
            web_portal_purchase__c as is_web_portal_purchase,
            opportunity_term_new__c as opportunity_term,
            pio__c as partner_initiated_opportunity,
            user_segment_o__c as user_segment,
            start_date__c::date as subscription_start_date,
            end_date__c::date as subscription_end_date,
            true_up_value__c as true_up_value,
            order_type_live__c as order_type_live,
            order_type_test__c as order_type_stamped,
            case
                when order_type_stamped = '1. New - First Order'
                then '1) New - First Order'
                when
                    order_type_stamped in (
                        '2. New - Connected',
                        '3. Growth',
                        '4. Contraction',
                        '5. Churn - Partial',
                        '6. Churn - Final'
                    )
                then '2) Growth (Growth / New - Connected / Churn / Contraction)'
                when order_type_stamped in ('7. PS / Other')
                then '3) Consumption / PS / Other'
                else 'Missing order_type_name_grouped'
            end as order_type_grouped,
            {{ growth_type("order_type_test__c", "arr_basis__c") }} as growth_type,
            arr_net__c as net_arr,
            arr_basis__c as arr_basis,
            arr__c as arr,
            days_in_sao__c as days_in_sao,
            new_logo_count__c as new_logo_count,
            {{ sales_hierarchy_sales_segment_cleaning("user_segment_o__c") }}
            as user_segment_stamped,
            case
                when user_segment_stamped in ('Large', 'PubSec')
                then 'Large'
                else user_segment_stamped
            end as user_segment_stamped_grouped,
            stamped_user_geo__c as user_geo_stamped,
            stamped_user_region__c as user_region_stamped,
            stamped_user_area__c as user_area_stamped,
            {{
                sales_segment_region_grouped(
                    "user_segment_stamped", "user_geo_stamped", "user_region_stamped"
                )
            }} as user_segment_region_stamped_grouped,
            concat(
                user_segment_stamped,
                '-',
                user_geo_stamped,
                '-',
                user_region_stamped,
                '-',
                user_area_stamped
            ) as user_segment_geo_region_area_stamped,
            stamped_opp_owner_user_role_type__c as crm_opp_owner_user_role_type_stamped,
            stamped_opportunity_owner__c as crm_opp_owner_stamped_name,
            stamped_account_owner__c as crm_account_owner_stamped_name,
            sao_opportunity_owner__c as sao_crm_opp_owner_stamped_name,
            sao_account_owner__c as sao_crm_account_owner_stamped_name,
            sao_user_segment__c as sao_crm_opp_owner_sales_segment_stamped,
            sao_opp_owner_segment_geo_region_area__c
            as sao_crm_opp_owner_sales_segment_geo_region_area_stamped,
            case
                when sao_crm_opp_owner_sales_segment_stamped in ('Large', 'PubSec')
                then 'Large'
                else sao_crm_opp_owner_sales_segment_stamped
            end as sao_crm_opp_owner_sales_segment_stamped_grouped,
            sao_user_geo__c as sao_crm_opp_owner_geo_stamped,
            sao_user_region__c as sao_crm_opp_owner_region_stamped,
            sao_user_area__c as sao_crm_opp_owner_area_stamped,
            {{
                sales_segment_region_grouped(
                    "sao_crm_opp_owner_sales_segment_stamped",
                    "sao_crm_opp_owner_geo_stamped",
                    "sao_crm_opp_owner_region_stamped",
                )
            }} as sao_crm_opp_owner_segment_region_stamped_grouped,
            opportunity_category__c as opportunity_category,
            opportunity_health__c as opportunity_health,
            risk_type__c as risk_type,
            risk_reasons__c as risk_reasons,
            tam_notes__c as tam_notes,
            solution_architect__c as primary_solution_architect,
            product_details__c as product_details,
            product_category__c as product_category,
            products_purchased__c as products_purchased,
            case
                when web_portal_purchase__c
                then 'Web Direct'
                when arr_net__c < 5000
                then '<5K'
                when arr_net__c < 25000
                then '5-25K'
                when arr_net__c < 100000
                then '25-100K'
                when arr_net__c < 250000
                then '100-250K'
                when arr_net__c > 250000
                then '250K+'
                else 'Missing opportunity_deal_size'
            end opportunity_deal_size,
            payment_schedule__c as payment_schedule,
            comp_y2_iacv__c as comp_y2_iacv,

            -- ************************************
            -- sales segmentation deprecated fields - 2020-09-03
            -- left temporary for the sake of MVC and avoid breaking SiSense existing
            -- charts
            coalesce(
                {{ sales_segment_cleaning("sales_segmentation_employees_o__c") }},
                'Unknown'
            ) as sales_segment,
            {{ sales_segment_cleaning("ultimate_parent_sales_segment_emp_o__c") }}
            as parent_segment,
            -- ************************************
            -- dates in stage fields
            days_in_0_pending_acceptance__c as days_in_0_pending_acceptance,
            days_in_1_discovery__c as days_in_1_discovery,
            days_in_2_scoping__c as days_in_2_scoping,
            days_in_3_technical_evaluation__c as days_in_3_technical_evaluation,
            days_in_4_proposal__c as days_in_4_proposal,
            days_in_5_negotiating__c as days_in_5_negotiating,

            x0_pending_acceptance_date__c as stage_0_pending_acceptance_date,
            x1_discovery_date__c as stage_1_discovery_date,
            x2_scoping_date__c as stage_2_scoping_date,
            x3_technical_evaluation_date__c as stage_3_technical_evaluation_date,
            x4_proposal_date__c as stage_4_proposal_date,
            x5_negotiating_date__c as stage_5_negotiating_date,
            x6_awaiting_signature_date__c as stage_6_awaiting_signature_date,
            x6_closed_won_date__c as stage_6_closed_won_date,
            x7_closed_lost_date__c as stage_6_closed_lost_date,

            -- sales segment fields
            coalesce(
                {{ sales_segment_cleaning("sales_segmentation_employees_o__c") }},
                'Unknown'
            ) as division_sales_segment_stamped,
            -- channel reporting
            -- original issue: https://gitlab.com/gitlab-data/analytics/-/issues/6072
            dr_partner_deal_type__c as dr_partner_deal_type,
            dr_partner_engagement__c as dr_partner_engagement,
            vartopiadrs__dr_deal_reg_id__c as dr_deal_id,
            vartopiadrs__primary_registration__c as dr_primary_registration,
            {{ channel_type("sqs_bucket_engagement", "order_type_stamped") }}
            as channel_type,
            impartnerprm__partneraccount__c as partner_account,
            vartopiadrs__dr_status1__c as dr_status,
            distributor__c as distributor,
            influence_partner__c as influence_partner,
            fulfillment_partner__c as fulfillment_partner,
            platform_partner__c as platform_partner,
            partner_track__c as partner_track,
            public_sector_opp__c::boolean as is_public_sector_opp,
            registration_from_portal__c::boolean as is_registration_from_portal,
            calculated_discount__c as calculated_discount,
            partner_discount__c as partner_discount,
            partner_discount_calc__c as partner_discount_calc,
            comp_channel_neutral__c as comp_channel_neutral,

            -- command plan fields
            fm_champion__c as cp_champion,
            fm_close_plan__c as cp_close_plan,
            fm_competition__c as cp_competition,
            fm_decision_criteria__c as cp_decision_criteria,
            fm_decision_process__c as cp_decision_process,
            fm_economic_buyer__c as cp_economic_buyer,
            fm_help__c as cp_help,
            fm_identify_pain__c as cp_identify_pain,
            fm_metrics__c as cp_metrics,
            fm_partner__c as cp_partner,
            fm_paper_process__c as cp_paper_process,
            fm_review_notes__c as cp_review_notes,
            fm_risks__c as cp_risks,
            fm_use_cases__c as cp_use_cases,
            fm_value_driver__c as cp_value_driver,
            fm_why_do_anything_at_all__c as cp_why_do_anything_at_all,
            fm_why_gitlab__c as cp_why_gitlab,
            fm_why_now__c as cp_why_now,

            -- original issue: https://gitlab.com/gitlab-data/analytics/-/issues/6577
            sa_validated_tech_evaluation_close_statu__c
            as sa_tech_evaluation_close_status,
            sa_validated_tech_evaluation_end_date__c as sa_tech_evaluation_end_date,
            sa_validated_tech_evaluation_start_date__c as sa_tech_evaluation_start_date,

            -- flag to identify eligible booking deals, excluding jihu - issue:
            -- https://gitlab.com/gitlab-com/sales-team/field-operations/systems/-/issues/1805
            fp_a_master_bookings_flag__c::boolean as fpa_master_bookings_flag,

            downgrade_reason__c as downgrade_reason,

            -- metadata
            convert_timezone(
                'America/Los_Angeles', convert_timezone('UTC', current_timestamp())
            ) as _last_dbt_run,
            datediff(
                days, lastactivitydate::date, current_date
            ) as days_since_last_activity,
            isdeleted as is_deleted,
            lastactivitydate as last_activity_date,
            recordtypeid as record_type_id

        from source
    )

select *
from renamed

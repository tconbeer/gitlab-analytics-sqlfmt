{{ config(tags=["mnpi"]) }}

with
    sfdc_opportunity_snapshots as (

        select * from {{ ref("sfdc_opportunity_snapshots_base") }}

    ),
    net_arr_net_iacv_conversion_factors as (

        select * from {{ ref("sheetload_net_arr_net_iacv_conversion_factors_source") }}

    ),
    final as (

        select
            -- keys
            date_actual,
            valid_from,
            valid_to,
            is_currently_valid,
            opportunity_snapshot_id,
            accountid as account_id,
            id as opportunity_id,
            name as opportunity_name,
            ownerid as owner_id,

            -- logistical information
            business_type__c as business_type,
            closedate::date as close_date,
            createddate::date as created_date,
            deployment_preference__c as deployment_preference,
            sql_source__c as generated_source,
            leadsource as lead_source,
            merged_opportunity__c as merged_opportunity_id,
            opportunity_owner__c as opportunity_owner,
            account_owner__c as opportunity_owner_manager,
            sales_market__c as opportunity_owner_department,
            sdr_lu__c as opportunity_sales_development_representative,
            bdr_lu__c as opportunity_business_development_representative,
            bdr_sdr__c as opportunity_development_representative,
            account_owner_team_o__c as account_owner_team_stamped,
            coalesce(
                {{ sales_segment_cleaning("ultimate_parent_sales_segment_emp_o__c") }},
                {{ sales_segment_cleaning("ultimate_parent_sales_segment_o__c") }}
            ) as parent_segment,
            sales_accepted_date__c as sales_accepted_date,
            engagement_type__c as sales_path,
            sales_qualified_date__c as sales_qualified_date,
            coalesce(
                {{ sales_segment_cleaning("sales_segmentation_employees_o__c") }},
                'Unknown'
            ) as sales_segment,
            type as sales_type,
            {{ sfdc_source_buckets("leadsource") }}
            stagename as stage_name,
            revenue_type__c as order_type,

            -- Stamped User Segment fields
            {{ sales_hierarchy_sales_segment_cleaning("user_segment_o__c") }}
            as user_segment_stamped,
            stamped_user_geo__c as user_geo_stamped,
            stamped_user_region__c as user_region_stamped,
            stamped_user_area__c as user_area_stamped,

            -- opportunity information
            acv_2__c as acv,
            iff(acv_2__c >= 0, 1, 0) as closed_deals,  -- so that you can exclude closed deals that had negative impact
            competitors__c as competitors,
            critical_deal_flag__c as critical_deal_flag,
            {{ sfdc_deal_size("incremental_acv_2__c", "deal_size") }},
            forecastcategoryname as forecast_category_name,
            incremental_acv_2__c as forecasted_iacv,
            iacv_created_date__c as iacv_created_date,
            incremental_acv__c as incremental_acv,
            invoice_number__c as invoice_number,
            is_refund_opportunity__c as is_refund,
            is_downgrade_opportunity__c as is_downgrade,
            swing_deal__c as is_swing_deal,
            is_edu_oss_opportunity__c as is_edu_oss,
            net_iacv__c as net_incremental_acv,
            nrv__c as nrv,
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
            sql_source__c as sales_qualified_source,
            iff(
                sales_qualified_source = 'Channel Generated',
                'Partner Sourced',
                'Co-sell'
            ) as sqs_bucket_engagement,
            solutions_to_be_replaced__c as solutions_to_be_replaced,
            amount as total_contract_value,
            upside_iacv__c as upside_iacv,
            upside_swing_deal_iacv__c as upside_swing_deal_iacv,
            web_portal_purchase__c as is_web_portal_purchase,
            opportunity_term__c as opportunity_term,
            opportunity_category__c as opportunity_category,
            arr_net__c as net_arr,
            case
                when closedate::date >= '2018-02-01'
                then coalesce((net_iacv__c * ratio_net_iacv_to_net_arr), net_iacv__c)
                else null
            end as net_arr_converted,
            case
                when closedate::date <= '2021-01-31' then net_arr_converted else net_arr
            end as net_arr_final,
            arr_basis__c as arr_basis,
            arr__c as arr,
            amount as amount,
            recurring_amount__c as recurring_amount,
            true_up_amount__c as true_up_amount,
            proserv_amount__c as proserv_amount,
            other_non_recurring_amount__c as other_non_recurring_amount,
            start_date__c::date as subscription_start_date,
            end_date__c::date as subscription_end_date,

            -- channel reporting
            -- original issue: https://gitlab.com/gitlab-data/analytics/-/issues/6072
            deal_path__c as deal_path,
            dr_partner_deal_type__c as dr_partner_deal_type,
            dr_partner_engagement__c as dr_partner_engagement,
            order_type_test__c as order_type_stamped,
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
            fm_identify_pain__c as cp_identify_pain,
            fm_metrics__c as cp_metrics,
            fm_risks__c as cp_risks,
            fm_use_cases__c as cp_use_cases,
            fm_value_driver__c as cp_value_driver,
            fm_why_do_anything_at_all__c as cp_why_do_anything_at_all,
            fm_why_gitlab__c as cp_why_gitlab,
            fm_why_now__c as cp_why_now,

            -- ************************************
            -- dates in stage fields
            x0_pending_acceptance_date__c as stage_0_pending_acceptance_date,
            x1_discovery_date__c as stage_1_discovery_date,
            x2_scoping_date__c as stage_2_scoping_date,
            x3_technical_evaluation_date__c as stage_3_technical_evaluation_date,
            x4_proposal_date__c as stage_4_proposal_date,
            x5_negotiating_date__c as stage_5_negotiating_date,
            x6_awaiting_signature_date__c as stage_6_awaiting_signature_date,
            x6_closed_won_date__c as stage_6_closed_won_date,
            x7_closed_lost_date__c as stage_6_closed_lost_date,

            -- flag to identify eligible booking deals, excluding jihu - issue:
            -- https://gitlab.com/gitlab-com/sales-team/field-operations/systems/-/issues/1805
            fp_a_master_bookings_flag__c as fpa_master_bookings_flag,

            -- metadata
            convert_timezone(
                'America/Los_Angeles', convert_timezone('UTC', current_timestamp())
            ) as _last_dbt_run,
            isdeleted as is_deleted,
            lastactivitydate as last_activity_date,
            recordtypeid as record_type_id
        from sfdc_opportunity_snapshots
        left join
            net_arr_net_iacv_conversion_factors
            on sfdc_opportunity_snapshots.id
            = net_arr_net_iacv_conversion_factors.opportunity_id

    )

select *
from final

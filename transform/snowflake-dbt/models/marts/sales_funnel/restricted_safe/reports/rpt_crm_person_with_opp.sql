{{
    simple_cte(
        [
            ("dim_crm_account", "dim_crm_account"),
            ("mart_crm_opportunity", "mart_crm_opportunity"),
            ("rpt_sdr_ramp_daily", "rpt_sdr_ramp_daily"),
            ("mart_crm_person", "mart_crm_person"),
            ("dim_crm_user", "dim_crm_user"),
        ]
    )
}},
upa_base as (select dim_parent_crm_account_id, dim_crm_account_id from dim_crm_account),
first_order_opps as (

    select * from mart_crm_opportunity where is_new_logo_first_order = true

),
accounts_with_first_order_opps as (

    select
        upa_base.dim_parent_crm_account_id,
        upa_base.dim_crm_account_id,
        first_order_opps.dim_crm_opportunity_id,
        false as is_first_order_available
    from upa_base
    left join
        first_order_opps
        on upa_base.dim_crm_account_id = first_order_opps.dim_crm_account_id
    where dim_crm_opportunity_id is not null

),
final as (

    select
        mart_crm_person.dim_crm_person_id,
        mart_crm_person.dim_crm_user_id,
        mart_crm_person.dim_crm_account_id,
        mart_crm_person.mql_date_first_id,
        mart_crm_person.mql_date_first,
        mart_crm_person.mql_datetime_first,
        mart_crm_person.mql_datetime_first_pt,
        mart_crm_person.mql_date_first_pt,
        mart_crm_person.mql_month_first,
        mart_crm_person.mql_month_first_pt,
        mart_crm_person.mql_date_lastest,
        mart_crm_person.mql_datetime_latest,
        mart_crm_person.mql_datetime_latest_pt,
        mart_crm_person.mql_date_lastest_pt,
        mart_crm_person.mql_month_latest,
        mart_crm_person.mql_month_latest_pt,
        mart_crm_person.created_date,
        mart_crm_person.created_date_pt,
        mart_crm_person.created_month,
        mart_crm_person.created_month_pt,
        mart_crm_person.lead_created_date,
        mart_crm_person.lead_created_date_pt,
        mart_crm_person.lead_created_month,
        mart_crm_person.lead_created_month_pt,
        mart_crm_person.contact_created_date,
        mart_crm_person.contact_created_date_pt,
        mart_crm_person.contact_created_month,
        mart_crm_person.contact_created_month_pt,
        mart_crm_person.true_inquiry_date,
        mart_crm_person.inquiry_date,
        mart_crm_person.inquiry_date_pt,
        mart_crm_person.inquiry_month,
        mart_crm_person.inquiry_month_pt,
        mart_crm_person.inquiry_inferred_date,
        mart_crm_person.inquiry_inferred_datetime,
        mart_crm_person.inquiry_inferred_date_pt,
        mart_crm_person.inquiry_inferred_month,
        mart_crm_person.inquiry_inferred_month_pt,
        mart_crm_person.accepted_date,
        mart_crm_person.accepted_datetime,
        mart_crm_person.accepted_datetime_pt,
        mart_crm_person.accepted_date_pt,
        mart_crm_person.accepted_month,
        mart_crm_person.accepted_month_pt,
        mart_crm_person.mql_sfdc_date,
        mart_crm_person.mql_sfdc_datetime,
        mart_crm_person.mql_sfdc_date_pt,
        mart_crm_person.mql_sfdc_month,
        mart_crm_person.mql_sfdc_month_pt,
        mart_crm_person.mql_inferred_date,
        mart_crm_person.mql_inferred_datetime,
        mart_crm_person.mql_inferred_date_pt,
        mart_crm_person.mql_inferred_month,
        mart_crm_person.mql_inferred_month_pt,
        mart_crm_person.qualifying_date,
        mart_crm_person.qualifying_date_pt,
        mart_crm_person.qualifying_month,
        mart_crm_person.qualifying_month_pt,
        mart_crm_person.qualified_date,
        mart_crm_person.qualified_date_pt,
        mart_crm_person.qualified_month,
        mart_crm_person.qualified_month_pt,
        mart_crm_person.converted_date,
        mart_crm_person.converted_date_pt,
        mart_crm_person.converted_month,
        mart_crm_person.converted_month_pt,
        mart_crm_person.worked_date,
        mart_crm_person.worked_date_pt,
        mart_crm_person.worked_month,
        mart_crm_person.worked_month_pt,
        mart_crm_person.email_domain,
        mart_crm_person.email_domain_type,
        mart_crm_person.email_hash,
        mart_crm_person.status,
        mart_crm_person.lead_source,
        mart_crm_person.source_buckets,
        mart_crm_person.crm_partner_id,
        mart_crm_person.sequence_step_type,
        mart_crm_person.account_demographics_geo as region,
        mart_crm_person.state,
        mart_crm_person.country,
        mart_crm_person.name_of_active_sequence,
        mart_crm_person.sequence_task_due_date,
        mart_crm_person.sequence_status,
        mart_crm_person.last_activity_date,
        mart_crm_person.is_actively_being_sequenced,
        mart_crm_person.bizible_marketing_channel_path_name,
        mart_crm_person.sales_segment_name,
        mart_crm_person.sales_segment_grouped,
        mart_crm_person.marketo_last_interesting_moment,
        mart_crm_person.marketo_last_interesting_moment_date,
        mart_crm_person.outreach_step_number,
        mart_crm_person.matched_account_owner_role,
        mart_crm_person.matched_account_account_owner_name,
        mart_crm_person.matched_account_sdr_assigned,
        mart_crm_person.matched_account_type,
        mart_crm_person.matched_account_gtm_strategy,
        mart_crm_person.account_demographics_sales_segment,
        mart_crm_person.account_demographics_geo,
        mart_crm_person.account_demographics_region,
        mart_crm_person.account_demographics_area,
        mart_crm_person.account_demographics_territory,
        mart_crm_person.account_demographics_employee_count,
        mart_crm_person.account_demographics_max_family_employee,
        mart_crm_person.account_demographics_upa_country,
        mart_crm_person.account_demographics_upa_state,
        mart_crm_person.account_demographics_upa_city,
        mart_crm_person.account_demographics_upa_street,
        mart_crm_person.account_demographics_upa_postal_code,
        mart_crm_person.account_demographics_sales_segment_grouped,
        mart_crm_person.account_demographics_segment_region_grouped,
        mart_crm_person.is_mql,
        mart_crm_person.is_inquiry,
        mart_crm_person.is_lead_source_trial,
        mart_crm_opportunity.dim_crm_opportunity_id,
        mart_crm_opportunity.created_date as opportunity_created_date,
        mart_crm_opportunity.sales_accepted_date,
        mart_crm_opportunity.close_date,
        mart_crm_opportunity.sales_qualified_source_name,
        mart_crm_opportunity.is_won,
        mart_crm_opportunity.net_arr,
        mart_crm_opportunity.is_edu_oss,
        mart_crm_opportunity.stage_name,
        mart_crm_opportunity.is_sao,
        case
            when dim_crm_user.crm_user_sales_segment = 'Other'
            then rpt_sdr_ramp_daily.sdr_segment
            else dim_crm_user.crm_user_sales_segment
        end as user_sales_segment,
        case
            when is_first_order_available = false
            then mart_crm_opportunity.order_type
            else '1. New - First Order'
        end as person_order_type,
        dim_crm_user.crm_user_region,
        dim_crm_user.crm_user_area,
        dim_crm_user.crm_user_geo
    from mart_crm_person
    left join
        mart_crm_opportunity
        on mart_crm_person.dim_crm_account_id = mart_crm_opportunity.dim_crm_account_id
    left join
        dim_crm_user on mart_crm_person.dim_crm_user_id = dim_crm_user.dim_crm_user_id
    left join
        rpt_sdr_ramp_daily
        on mart_crm_person.dim_crm_user_id = rpt_sdr_ramp_daily.dim_crm_user_id
    left join
        upa_base on mart_crm_person.dim_crm_account_id = upa_base.dim_crm_account_id
    left join
        accounts_with_first_order_opps
        on upa_base.dim_parent_crm_account_id
        = accounts_with_first_order_opps.dim_parent_crm_account_id

)

{{
    dbt_audit(
        cte_ref="final",
        created_by="@rkohnke",
        updated_by="@michellecooper",
        created_date="2022-01-20",
        updated_date="2022-03-30",
    )
}}

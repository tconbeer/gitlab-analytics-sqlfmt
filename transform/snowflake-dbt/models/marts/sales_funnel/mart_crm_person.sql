{{ config(tags=["mnpi_exception"]) }}

{{ config({"schema": "common_mart_marketing"}) }}

{{
    simple_cte(
        [
            ("dim_crm_person", "dim_crm_person"),
            (
                "dim_bizible_marketing_channel_path",
                "dim_bizible_marketing_channel_path",
            ),
            ("dim_sales_segment", "dim_sales_segment"),
            ("fct_crm_person", "fct_crm_person"),
            ("dim_date", "dim_date"),
        ]
    )
}},
final as (

    select
        fct_crm_person.dim_crm_person_id,
        dim_crm_person.dim_crm_user_id,
        dim_crm_person.dim_crm_account_id,
        mql_date_first.date_id as mql_date_first_id,
        mql_date_first.date_day as mql_date_first,
        fct_crm_person.mql_datetime_first,
        fct_crm_person.mql_datetime_first_pt,
        mql_date_first_pt.date_day as mql_date_first_pt,
        mql_date_first.first_day_of_month as mql_month_first,
        mql_date_first_pt.first_day_of_month as mql_month_first_pt,
        mql_date_latest.date_day as mql_date_lastest,
        fct_crm_person.mql_datetime_latest,
        fct_crm_person.mql_datetime_latest_pt,
        mql_date_latest_pt.date_day as mql_date_lastest_pt,
        mql_date_latest.first_day_of_month as mql_month_latest,
        mql_date_latest_pt.first_day_of_month as mql_month_latest_pt,
        created_date.date_day as created_date,
        created_date_pt.date_day as created_date_pt,
        created_date.first_day_of_month as created_month,
        created_date_pt.first_day_of_month as created_month_pt,
        lead_created_date.date_day as lead_created_date,
        lead_created_date_pt.date_day as lead_created_date_pt,
        lead_created_date.first_day_of_month as lead_created_month,
        lead_created_date_pt.first_day_of_month as lead_created_month_pt,
        contact_created_date.date_day as contact_created_date,
        contact_created_date_pt.date_day as contact_created_date_pt,
        contact_created_date.first_day_of_month as contact_created_month,
        contact_created_date_pt.first_day_of_month as contact_created_month_pt,
        true_inquiry_date as true_inquiry_date,
        inquiry_date.date_day as inquiry_date,
        inquiry_date_pt.date_day as inquiry_date_pt,
        inquiry_date.first_day_of_month as inquiry_month,
        inquiry_date_pt.first_day_of_month as inquiry_month_pt,
        inquiry_inferred_datetime.date_day as inquiry_inferred_date,
        fct_crm_person.inquiry_inferred_datetime,
        inquiry_inferred_datetime_pt.date_day as inquiry_inferred_date_pt,
        inquiry_inferred_datetime.first_day_of_month as inquiry_inferred_month,
        inquiry_inferred_datetime.first_day_of_month as inquiry_inferred_month_pt,
        accepted_date.date_day as accepted_date,
        fct_crm_person.accepted_datetime,
        fct_crm_person.accepted_datetime_pt,
        accepted_date_pt.date_day as accepted_date_pt,
        accepted_date.first_day_of_month as accepted_month,
        accepted_date_pt.first_day_of_month as accepted_month_pt,
        mql_sfdc_date.date_day as mql_sfdc_date,
        fct_crm_person.mql_sfdc_datetime,
        mql_sfdc_date_pt.date_day as mql_sfdc_date_pt,
        mql_sfdc_date.first_day_of_month as mql_sfdc_month,
        mql_sfdc_date_pt.first_day_of_month as mql_sfdc_month_pt,
        mql_inferred_date.date_day as mql_inferred_date,
        fct_crm_person.mql_inferred_datetime,
        mql_inferred_date_pt.date_day as mql_inferred_date_pt,
        mql_inferred_date.first_day_of_month as mql_inferred_month,
        mql_inferred_date_pt.first_day_of_month as mql_inferred_month_pt,
        qualifying_date.date_day as qualifying_date,
        qualifying_date_pt.date_day as qualifying_date_pt,
        qualifying_date.first_day_of_month as qualifying_month,
        qualifying_date_pt.first_day_of_month as qualifying_month_pt,
        qualified_date.date_day as qualified_date,
        qualified_date_pt.date_day as qualified_date_pt,
        qualified_date.first_day_of_month as qualified_month,
        qualified_date_pt.first_day_of_month as qualified_month_pt,
        converted_date.date_day as converted_date,
        converted_date_pt.date_day as converted_date_pt,
        converted_date.first_day_of_month as converted_month,
        converted_date_pt.first_day_of_month as converted_month_pt,
        worked_date.date_day as worked_date,
        worked_date_pt.date_day as worked_date_pt,
        worked_date.first_day_of_month as worked_month,
        worked_date_pt.first_day_of_month as worked_month_pt,
        dim_crm_person.email_domain,
        dim_crm_person.email_domain_type,
        dim_crm_person.email_hash,
        dim_crm_person.status,
        dim_crm_person.lead_source,
        dim_crm_person.source_buckets,
        dim_crm_person.crm_partner_id,
        dim_crm_person.prospect_share_status,
        dim_crm_person.partner_prospect_status,
        dim_crm_person.partner_prospect_owner_name,
        dim_crm_person.partner_prospect_id,
        dim_crm_person.sequence_step_type,
        dim_crm_person.state,
        dim_crm_person.country,
        fct_crm_person.name_of_active_sequence,
        fct_crm_person.sequence_task_due_date,
        fct_crm_person.sequence_status,
        fct_crm_person.last_activity_date,
        dim_crm_person.is_actively_being_sequenced,
        dim_bizible_marketing_channel_path.bizible_marketing_channel_path_name,
        dim_sales_segment.sales_segment_name,
        dim_sales_segment.sales_segment_grouped,
        dim_crm_person.marketo_last_interesting_moment,
        dim_crm_person.marketo_last_interesting_moment_date,
        dim_crm_person.outreach_step_number,
        dim_crm_person.matched_account_owner_role,
        dim_crm_person.matched_account_account_owner_name,
        dim_crm_person.matched_account_sdr_assigned,
        dim_crm_person.matched_account_type,
        dim_crm_person.matched_account_gtm_strategy,
        fct_crm_person.account_demographics_sales_segment,
        fct_crm_person.account_demographics_sales_segment_grouped,
        fct_crm_person.account_demographics_geo,
        fct_crm_person.account_demographics_region,
        fct_crm_person.account_demographics_area,
        fct_crm_person.account_demographics_segment_region_grouped,
        fct_crm_person.account_demographics_territory,
        fct_crm_person.account_demographics_employee_count,
        fct_crm_person.account_demographics_max_family_employee,
        fct_crm_person.account_demographics_upa_country,
        fct_crm_person.account_demographics_upa_state,
        fct_crm_person.account_demographics_upa_city,
        fct_crm_person.account_demographics_upa_street,
        fct_crm_person.account_demographics_upa_postal_code,
        fct_crm_person.is_mql,
        fct_crm_person.is_inquiry,
        case
            when lower(dim_crm_person.lead_source) like '%trial - gitlab.com%'
            then true
            when lower(dim_crm_person.lead_source) like '%trial - enterprise%'
            then true
            else false
        end as is_lead_source_trial
    from fct_crm_person
    left join
        dim_crm_person
        on fct_crm_person.dim_crm_person_id = dim_crm_person.dim_crm_person_id
    left join
        dim_sales_segment
        on fct_crm_person.dim_account_sales_segment_id
        = dim_sales_segment.dim_sales_segment_id
    left join
        dim_bizible_marketing_channel_path
        on fct_crm_person.dim_bizible_marketing_channel_path_id
        = dim_bizible_marketing_channel_path.dim_bizible_marketing_channel_path_id
    left join
        dim_date as created_date
        on fct_crm_person.created_date_id = created_date.date_id
    left join
        dim_date as created_date_pt
        on fct_crm_person.created_date_pt_id = created_date_pt.date_id
    left join
        dim_date as lead_created_date
        on fct_crm_person.lead_created_date_id = lead_created_date.date_id
    left join
        dim_date as lead_created_date_pt
        on fct_crm_person.lead_created_date_pt_id = lead_created_date_pt.date_id
    left join
        dim_date as contact_created_date
        on fct_crm_person.contact_created_date_id = contact_created_date.date_id
    left join
        dim_date as contact_created_date_pt
        on fct_crm_person.contact_created_date_pt_id = contact_created_date_pt.date_id
    left join
        dim_date as inquiry_date
        on fct_crm_person.inquiry_date_id = inquiry_date.date_id
    left join
        dim_date as inquiry_date_pt
        on fct_crm_person.inquiry_date_pt_id = inquiry_date_pt.date_id
    left join
        dim_date as inquiry_inferred_datetime
        on fct_crm_person.inquiry_inferred_datetime_id
        = inquiry_inferred_datetime.date_id
    left join
        dim_date as inquiry_inferred_datetime_pt
        on fct_crm_person.inquiry_inferred_datetime_pt_id
        = inquiry_inferred_datetime_pt.date_id
    left join
        dim_date as mql_date_first
        on fct_crm_person.mql_date_first_id = mql_date_first.date_id
    left join
        dim_date as mql_date_first_pt
        on fct_crm_person.mql_date_first_pt_id = mql_date_first_pt.date_id
    left join
        dim_date as mql_date_latest
        on fct_crm_person.mql_date_latest_id = mql_date_latest.date_id
    left join
        dim_date as mql_date_latest_pt
        on fct_crm_person.mql_date_latest_pt_id = mql_date_latest_pt.date_id
    left join
        dim_date as mql_sfdc_date
        on fct_crm_person.mql_sfdc_date_id = mql_sfdc_date.date_id
    left join
        dim_date as mql_sfdc_date_pt
        on fct_crm_person.mql_sfdc_date_pt_id = mql_sfdc_date_pt.date_id
    left join
        dim_date as mql_inferred_date
        on fct_crm_person.mql_inferred_date_id = mql_inferred_date.date_id
    left join
        dim_date as mql_inferred_date_pt
        on fct_crm_person.mql_inferred_date_pt_id = mql_inferred_date_pt.date_id
    left join
        dim_date as accepted_date
        on fct_crm_person.accepted_date_id = accepted_date.date_id
    left join
        dim_date as accepted_date_pt
        on fct_crm_person.accepted_date_pt_id = accepted_date_pt.date_id
    left join
        dim_date as qualified_date
        on fct_crm_person.qualified_date_id = qualified_date.date_id
    left join
        dim_date as qualified_date_pt
        on fct_crm_person.qualified_date_pt_id = qualified_date_pt.date_id
    left join
        dim_date as qualifying_date
        on fct_crm_person.qualifying_date_id = qualifying_date.date_id
    left join
        dim_date as qualifying_date_pt
        on fct_crm_person.qualifying_date_pt_id = qualifying_date_pt.date_id
    left join
        dim_date converted_date
        on fct_crm_person.converted_date_id = converted_date.date_id
    left join
        dim_date converted_date_pt
        on fct_crm_person.converted_date_pt_id = converted_date_pt.date_id
    left join
        dim_date as worked_date on fct_crm_person.worked_date_id = worked_date.date_id
    left join
        dim_date as worked_date_pt
        on fct_crm_person.worked_date_pt_id = worked_date_pt.date_id

)

{{
    dbt_audit(
        cte_ref="final",
        created_by="@iweeks",
        updated_by="@jpeguero",
        created_date="2020-12-07",
        updated_date="2022-03-17",
    )
}}

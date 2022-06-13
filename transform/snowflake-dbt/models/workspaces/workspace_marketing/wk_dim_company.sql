{{ config(materialized="table", tags=["mnpi_exception"]) }}

{{
    simple_cte(
        [
            ("accounts", "sfdc_account_source"),
            ("contacts", "sfdc_contact_source"),
            ("leads", "sfdc_lead_source"),
            ("zoom_info", "zi_comp_with_linkages_global_source"),
        ]
    )
}},

salesforce_accounts as (
    select
        zoom_info_dozisf_zi_id as company_id,
        zoom_info_company_name as company_name,
        zoom_info_company_revenue as company_revenue,
        zoom_info_company_employee_count as company_employee_count,
        zoom_info_company_industry as company_industry,
        zoom_info_company_state_province as company_state_province,
        zoom_info_company_country as company_country,
        iff(company_industry is not null, 1, 0) + iff(
            company_state_province is not null, 1, 0
        ) + iff(company_country is not null, 1, 0) as completeness_score
    from accounts
    where company_id is not null and is_excluded_from_zoom_info_enrich = false
    qualify
        max(company_revenue) over (
            partition by company_id
        ) = company_revenue and row_number() over (
            partition by company_id order by completeness_score desc
        ) = 1
),

salesforce_leads as (
    select
        zoominfo_company_id as company_id,
        company as company_name,
        zoominfo_company_revenue as company_revenue,
        zoominfo_company_employee_count as company_employee_count,
        zoominfo_company_industry as company_industry,
        zoominfo_company_state as company_state_province,
        zoominfo_company_country as company_country,
        iff(company_industry is not null, 1, 0) + iff(
            company_state_province is not null, 1, 0
        ) + iff(company_country is not null, 1, 0) as completeness_score
    from leads
    where company_id is not null
    qualify
        max(company_revenue) over (
            partition by company_id
        ) = company_revenue and row_number() over (
            partition by company_id order by completeness_score desc
        ) = 1
),

salesforce_contacts as (
    select
        zoominfo_company_id as company_id,
        zoominfo_company_revenue as company_revenue,
        zoominfo_company_employee_count as company_employee_count,
        zoominfo_company_industry as company_industry,
        zoominfo_company_state_province as company_state_province,
        zoominfo_company_country as company_country,
        iff(company_industry is not null, 1, 0) + iff(
            company_state_province is not null, 1, 0
        ) + iff(company_country is not null, 1, 0) as completeness_score
    from contacts
    where company_id is not null
    qualify
        max(company_revenue) over (
            partition by company_id
        ) = company_revenue and row_number() over (
            partition by company_id order by completeness_score desc
        ) = 1
),

zoom_info_base as (
    select
        company_id as company_id,
        headquarters_company_name as company_name,
        headquarters_employees as company_employee_count,
        industry_primary as company_industry,
        headquarters_company_state as company_state_province,
        headquarters_company_country as company_country,
        merged_previous_company_ids,
        headquarters_revenue as company_revenue
    from zoom_info
    where is_headquarters = true
),

zoom_info_merged as (
    select distinct
        merged_company_ids.value::varchar as company_id,
        zoom_info_base.company_name,
        zoom_info_base.company_revenue,
        zoom_info_base.company_employee_count,
        zoom_info_base.company_industry,
        zoom_info_base.company_state_province,
        zoom_info_base.company_country,
        zoom_info_base.company_id as source_company_id
    from zoom_info_base
    inner join
        lateral flatten(
            input => split(merged_previous_company_ids, '|')
        ) as merged_company_ids
),

company_id_spine as (

    select company_id
    from salesforce_accounts

    union

    select company_id
    from salesforce_leads

    union

    select company_id
    from salesforce_contacts

    union

    select company_id
    from zoom_info_base

    union

    select company_id
    from zoom_info_merged

),

report as (
    select distinct
        {{ dbt_utils.surrogate_key(["company_id_spine.company_id::INT"]) }}
        as dim_company_id,
        company_id_spine.company_id::int as company_id,
        zoom_info_merged.source_company_id,
        coalesce(
            zoom_info_base.company_name,
            zoom_info_merged.company_name,
            salesforce_accounts.company_name,
            salesforce_leads.company_name,
            'Unknown Company Name'
        ) as company_name,
        coalesce(
            zoom_info_base.company_revenue,
            zoom_info_merged.company_revenue,
            salesforce_accounts.company_revenue,
            salesforce_contacts.company_revenue,
            salesforce_leads.company_revenue
        ) as company_revenue,
        coalesce(
            zoom_info_base.company_employee_count,
            zoom_info_merged.company_employee_count,
            salesforce_accounts.company_employee_count,
            salesforce_contacts.company_employee_count,
            salesforce_leads.company_employee_count
        ) as company_employee_count,
        coalesce(
            zoom_info_base.company_industry,
            zoom_info_merged.company_industry,
            salesforce_accounts.company_industry,
            salesforce_contacts.company_industry,
            salesforce_leads.company_industry
        ) as company_industry,
        coalesce(
            zoom_info_base.company_country,
            zoom_info_merged.company_country,
            salesforce_accounts.company_country,
            salesforce_contacts.company_country,
            salesforce_leads.company_country
        ) as company_country,
        coalesce(
            zoom_info_base.company_state_province,
            zoom_info_merged.company_state_province,
            salesforce_accounts.company_state_province,
            salesforce_contacts.company_state_province,
            salesforce_leads.company_state_province
        ) as company_state_province,
        iff(salesforce_accounts.company_id is not null, true, false) as has_crm_account,
        iff(salesforce_leads.company_id is not null, true, false) as has_crm_lead,
        iff(salesforce_contacts.company_id is not null, true, false) as has_crm_contact,
        iff(zoom_info_base.company_id is not null, true, false) as is_company_hq,
        iff(
            zoom_info_merged.company_id is not null, true, false
        ) as is_merged_company_id
    from company_id_spine
    left join zoom_info_base on company_id_spine.company_id = zoom_info_base.company_id
    left join
        salesforce_accounts
        on company_id_spine.company_id = salesforce_accounts.company_id
    left join
        zoom_info_merged on company_id_spine.company_id = zoom_info_merged.company_id
    left join
        salesforce_leads on company_id_spine.company_id = salesforce_leads.company_id
    left join
        salesforce_contacts
        on company_id_spine.company_id = salesforce_contacts.company_id
    where company_id_spine.company_id is not null
)

select *
from report

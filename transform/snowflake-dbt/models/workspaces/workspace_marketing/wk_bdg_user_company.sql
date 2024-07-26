{{ config(materialized="table") }}

{{
    simple_cte(
        [
            ("users", "gitlab_dotcom_users_source"),
            ("users_enhance", "gitlab_contact_enhance_source"),
        ]
    )
}},

sf_leads as (

    select zoominfo_company_id, lead_email
    from {{ ref("sfdc_lead_source") }}
    where zoominfo_company_id is not null
    -- email is not unique, use the record created most recently
    qualify row_number() over (partition by lead_email order by created_date desc) = 1
),

sf_contacts as (

    select zoominfo_company_id, contact_email
    from {{ ref("sfdc_contact_source") }}
    where zoominfo_company_id is not null
    -- email is not unique, use the record created most recently
    qualify
        row_number() over (partition by contact_email order by created_date desc) = 1
),

rpt as (

    select distinct
        users.user_id as gitlab_dotcom_user_id,
        coalesce(
            sf_leads.zoominfo_company_id,
            sf_contacts.zoominfo_company_id,
            users_enhance.zoominfo_company_id
        ) as company_id,
        sf_leads.zoominfo_company_id as sf_lead_company_id,
        sf_contacts.zoominfo_company_id as sf_contact_company_id,
        users_enhance.zoominfo_company_id as gitlab_user_enhance_company_id,
        {{ dbt_utils.surrogate_key(["users.user_id"]) }} as dim_user_id,
        {{ dbt_utils.surrogate_key(["company_id"]) }} as dim_company_id
    from users
    left join sf_leads on users.email = sf_leads.lead_email
    left join sf_contacts on users.email = sf_contacts.contact_email
    left join users_enhance on users.user_id = users_enhance.user_id
    where company_id is not null

)

select *
from rpt

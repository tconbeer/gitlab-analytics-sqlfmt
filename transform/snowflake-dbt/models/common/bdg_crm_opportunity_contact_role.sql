with
    contact_role as (select * from {{ ref("sfdc_opportunity_contact_role_source") }}),
    crm_person as (select * from {{ ref("prep_crm_person") }}),
    final_opportunity_contacts as (

        select

            contact_role.opportunity_contact_role_id,
            crm_person.dim_crm_person_id,
            crm_person.sfdc_record_id,
            contact_role.contact_id,
            contact_role.opportunity_id as dim_crm_opportunity_id,
            contact_role.created_by_id,
            contact_role.last_modified_by_id,
            contact_role.contact_role,
            contact_role.is_primary_contact,
            contact_role.created_date,
            {{ get_date_id("contact_role.created_date") }} as created_date_id,
            contact_role.last_modified_date,
            {{ get_date_id("contact_role.last_modified_date") }}
            as last_modified_date_id

        from contact_role
        left join crm_person on contact_role.contact_id = crm_person.sfdc_record_id
    )

    {{
        dbt_audit(
            cte_ref="final_opportunity_contacts",
            created_by="@mcooperDD",
            updated_by="@mcooperDD",
            created_date="2020-11-20",
            updated_date="2020-11-20",
        )
    }}

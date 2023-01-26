with
    biz_person as (

        select * from {{ ref("sfdc_bizible_person_source") }} where is_deleted = 'FALSE'

    ),
    biz_touchpoints as (

        select *
        from {{ ref("sfdc_bizible_touchpoint_source") }}
        where bizible_touchpoint_position like '%FT%' and is_deleted = 'FALSE'

    ),
    biz_person_with_touchpoints as (

        select

            biz_touchpoints.*, biz_person.bizible_contact_id, biz_person.bizible_lead_id

        from biz_touchpoints
        join biz_person on biz_touchpoints.bizible_person_id = biz_person.person_id

    ),
    sfdc_contacts as (

        select {{ hash_sensitive_columns("sfdc_contact_source") }}
        from {{ ref("sfdc_contact_source") }}
        where is_deleted = 'FALSE'

    ),
    sfdc_leads as (

        select {{ hash_sensitive_columns("sfdc_lead_source") }}
        from {{ ref("sfdc_lead_source") }}
        where is_deleted = 'FALSE'

    ),
    crm_person_final as (

        select
            -- id
            {{ dbt_utils.surrogate_key(["contact_id"]) }} as dim_crm_person_id,
            contact_id as sfdc_record_id,
            bizible_person_id as bizible_person_id,
            'contact' as sfdc_record_type,
            contact_email_hash as email_hash,
            email_domain,
            email_domain_type,

            -- keys
            master_record_id,
            owner_id,
            record_type_id,
            account_id as dim_crm_account_id,
            reports_to_id,
            owner_id as dim_crm_user_id,

            -- info
            person_score,
            contact_title as title,
            it_job_title_hierarchy,
            has_opted_out_email,
            email_bounced_date,
            email_bounced_reason,
            contact_status as status,
            lead_source,
            lead_source_type,
            source_buckets,
            net_new_source_categories,
            bizible_touchpoint_position,
            bizible_marketing_channel_path,
            bizible_touchpoint_date,
            marketo_last_interesting_moment,
            marketo_last_interesting_moment_date,
            outreach_step_number,
            null as matched_account_owner_role,
            null as matched_account_account_owner_name,
            null as matched_account_sdr_assigned,
            null as matched_account_type,
            null as matched_account_gtm_strategy,
            last_utm_content,
            last_utm_campaign,
            sequence_step_type,
            name_of_active_sequence,
            sequence_task_due_date,
            sequence_status,
            is_actively_being_sequenced,
            prospect_share_status,
            partner_prospect_status,
            partner_prospect_id,
            partner_prospect_owner_name,
            mailing_country as country,
            mailing_state as state,
            last_activity_date,
            account_demographics_sales_segment,
            account_demographics_sales_segment_grouped,
            account_demographics_geo,
            account_demographics_region,
            account_demographics_area,
            account_demographics_segment_region_grouped,
            account_demographics_territory,
            account_demographics_employee_count,
            account_demographics_max_family_employee,
            account_demographics_upa_country,
            account_demographics_upa_state,
            account_demographics_upa_city,
            account_demographics_upa_street,
            account_demographics_upa_postal_code,

            null as crm_partner_id

        from sfdc_contacts
        left join
            biz_person_with_touchpoints
            on sfdc_contacts.contact_id = biz_person_with_touchpoints.bizible_contact_id

        union

        select
            -- id
            {{ dbt_utils.surrogate_key(["lead_id"]) }} as dim_crm_person_id,
            lead_id as sfdc_record_id,
            bizible_person_id as bizible_person_id,
            'lead' as sfdc_record_type,
            lead_email_hash as email_hash,
            email_domain,
            email_domain_type,

            -- keys
            master_record_id,
            owner_id,
            record_type_id,
            lean_data_matched_account as dim_crm_account_id,
            null as reports_to_id,
            owner_id as dim_crm_user_id,

            -- info
            person_score,
            title,
            it_job_title_hierarchy,
            has_opted_out_email,
            email_bounced_date,
            email_bounced_reason,
            lead_status as status,
            lead_source,
            lead_source_type,
            source_buckets,
            net_new_source_categories,
            bizible_touchpoint_position,
            bizible_marketing_channel_path,
            bizible_touchpoint_date,
            marketo_last_interesting_moment,
            marketo_last_interesting_moment_date,
            outreach_step_number,
            matched_account_owner_role,
            matched_account_account_owner_name,
            matched_account_sdr_assigned,
            matched_account_type,
            matched_account_gtm_strategy,
            last_utm_content,
            last_utm_campaign,
            sequence_step_type,
            name_of_active_sequence,
            sequence_task_due_date,
            sequence_status,
            is_actively_being_sequenced,
            prospect_share_status,
            partner_prospect_status,
            partner_prospect_id,
            partner_prospect_owner_name,
            country,
            state,
            last_activity_date,
            account_demographics_sales_segment,
            account_demographics_sales_segment_grouped,
            account_demographics_geo,
            account_demographics_region,
            account_demographics_area,
            account_demographics_segment_region_grouped,
            account_demographics_territory,
            account_demographics_employee_count,
            account_demographics_max_family_employee,
            account_demographics_upa_country,
            account_demographics_upa_state,
            account_demographics_upa_city,
            account_demographics_upa_street,
            account_demographics_upa_postal_code,
            crm_partner_id

        from sfdc_leads
        left join
            biz_person_with_touchpoints
            on sfdc_leads.lead_id = biz_person_with_touchpoints.bizible_lead_id
        where is_converted = 'FALSE'

    ),
    duplicates as (

        select dim_crm_person_id from crm_person_final group by 1 having count(*) > 1

    ),
    final as (

        select *
        from crm_person_final
        where
            dim_crm_person_id not in (select * from duplicates)
            and sfdc_record_id
            -- DQ issue: https://gitlab.com/gitlab-data/analytics/-/issues/11559
            != '00Q4M00000kDDKuUAO'

    )

    {{
        dbt_audit(
            cte_ref="final",
            created_by="@mcooperDD",
            updated_by="@jpeguero",
            created_date="2020-12-08",
            updated_date="2022-03-26",
        )
    }}

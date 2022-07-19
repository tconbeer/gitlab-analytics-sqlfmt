with
    source as (select * from {{ source("salesforce", "contact") }}),

    renamed as (

        select
            -- id
            id as contact_id,
            name as contact_name,
            firstname as contact_first_name,
            lastname as contact_last_name,
            email as contact_email,
            split_part(email, '@', 2) as email_domain,
            {{ email_domain_type("split_part(email,'@',2)", "leadsource") }}
            as email_domain_type,

            -- keys
            accountid as account_id,
            masterrecordid as master_record_id,
            ownerid as owner_id,
            recordtypeid as record_type_id,
            reportstoid as reports_to_id,

            -- contact info
            title as contact_title,
            {{ it_job_title_hierarchy("title") }},
            role__c as contact_role,
            mobilephone as mobile_phone,
            mkto71_lead_score__c as person_score,

            department as department,
            contact_status__c as contact_status,
            requested_contact__c as requested_contact,
            inactive_contact__c as inactive_contact,
            hasoptedoutofemail as has_opted_out_email,
            invalid_email_address__c as invalid_email_address,
            isemailbounced as email_is_bounced,
            emailbounceddate as email_bounced_date,
            emailbouncedreason as email_bounced_reason,

            mailingstreet as mailing_address,
            mailingcity as mailing_city,
            mailingstate as mailing_state,
            mailingstatecode as mailing_state_code,
            mailingcountry as mailing_country,
            mailingcountrycode as mailing_country_code,
            mailingpostalcode as mailing_zip_code,

            -- info
            dozisf__zoominfo_company_id__c as zoominfo_company_id,
            zi_company_revenue__c as zoominfo_company_revenue,
            zi_employee_count__c as zoominfo_company_employee_count,
            zi_company_city__c as zoominfo_company_city,
            zi_industry__c as zoominfo_company_industry,
            zi_company_state__c as zoominfo_company_state_province,
            zi_company_country__c as zoominfo_company_country,
            using_ce__c as using_ce,
            ee_trial_start_date__c as ee_trial_start_date,
            ee_trial_end_date__c as ee_trial_end_date,
            industry__c as industry,
            -- maybe we can exclude this if it's not relevant
            responded_to_githost_price_change__c as responded_to_githost_price_change,
            core_check_in_notes__c as core_check_in_notes,
            leadsource as lead_source,
            lead_source_type__c as lead_source_type,
            outreach_stage__c as outreach_stage,
            sequence_step_number__c as outreach_step_number,
            account_type__c as account_type,
            contact_assigned_datetime__c::timestamp as assigned_datetime,
            mql_timestamp__c as marketo_qualified_lead_timestamp,
            mql_datetime__c as marketo_qualified_lead_datetime,
            mql_date__c as marketo_qualified_lead_date,
            mql_datetime_inferred__c as mql_datetime_inferred,
            inquiry_datetime__c as inquiry_datetime,
            inquiry_datetime_inferred__c as inquiry_datetime_inferred,
            accepted_datetime__c as accepted_datetime,
            qualifying_datetime__c as qualifying_datetime,
            qualified_datetime__c as qualified_datetime,
            unqualified_datetime__c as unqualified_datetime,
            nurture_datetime__c as nurture_datetime,
            bad_data_datetime__c as bad_data_datetime,
            worked_date__c as worked_datetime,
            web_portal_purchase_datetime__c as web_portal_purchase_datetime,
            mkto_si__last_interesting_moment__c as marketo_last_interesting_moment,
            mkto_si__last_interesting_moment_date__c
            as marketo_last_interesting_moment_date,
            last_utm_campaign__c as last_utm_campaign,
            last_utm_content__c as last_utm_content,
            vartopiadrs__partner_prospect_acceptance__c as prospect_share_status,
            vartopiadrs__partner_prospect_status__c as partner_prospect_status,
            vartopiadrs__vartopia_prospect_id__c as partner_prospect_id,
            vartopiadrs__partner_prospect_owner_name__c as partner_prospect_owner_name,
            sequence_step_type2__c as sequence_step_type,
            name_of_active_sequence__c as name_of_active_sequence,
            sequence_task_due_date__c::date as sequence_task_due_date,
            sequence_status__c as sequence_status,
            actively_being_sequenced__c::boolean as is_actively_being_sequenced,
            {{ sfdc_source_buckets("leadsource") }}


            -- account demographics fields
            account_demographics_sales_segment__c as account_demographics_sales_segment,
            case
                when account_demographics_sales_segment__c in ('Large', 'PubSec')
                then 'Large'
                else account_demographics_sales_segment__c
            end as account_demographics_sales_segment_grouped,
            account_demographics_geo__c as account_demographics_geo,
            account_demographics_region__c as account_demographics_region,
            account_demographics_area__c as account_demographics_area,
            {{
                sales_segment_region_grouped(
                    "account_demographics_sales_segment__c",
                    "account_demographics_geo__c",
                    "account_demographics_region__c",
                )
            }} as account_demographics_segment_region_grouped,
            account_demographics_territory__c as account_demographics_territory,
            account_demographic_employee_count__c
            as account_demographics_employee_count,
            account_demographics_max_family_employe__c
            as account_demographics_max_family_employee,
            account_demographics_upa_country__c as account_demographics_upa_country,
            account_demographics_upa_state__c as account_demographics_upa_state,
            account_demographics_upa_city__c as account_demographics_upa_city,
            account_demographics_upa_street__c as account_demographics_upa_street,
            account_demographics_upa_postal_code__c
            as account_demographics_upa_postal_code,

            -- path factory info
            pathfactory_experience_name__c as pathfactory_experience_name,
            pathfactory_engagement_score__c as pathfactory_engagement_score,
            pathfactory_content_count__c as pathfactory_content_count,
            pathfactory_content_list__c as pathfactory_content_list,
            pathfactory_content_journey__c as pathfactory_content_journey,
            pathfactory_topic_list__c as pathfactory_topic_list,

            -- gl info
            account_owner__c as account_owner,
            ae_comments__c as ae_comments,
            business_development_rep__c as business_development_rep_name,
            outbound_bdr__c as outbound_business_development_rep_name,

            -- metadata
            createdbyid as created_by_id,
            createddate as created_date,
            isdeleted as is_deleted,
            lastactivitydate::date as last_activity_date,
            lastcurequestdate as last_cu_request_date,
            lastcuupdatedate as last_cu_update_date,
            lastmodifiedbyid as last_modified_by_id,
            lastmodifieddate as last_modified_date,
            systemmodstamp

        from source

    )

select *
from renamed

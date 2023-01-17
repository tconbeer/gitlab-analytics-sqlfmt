with
    source as (select * from {{ source("salesforce", "lead") }}),

    renamed as (

        select
            -- id
            id as lead_id,
            name as lead_name,
            firstname as lead_first_name,
            lastname as lead_last_name,
            email as lead_email,
            split_part(email, '@', 2) as email_domain,
            {{ email_domain_type("split_part(email,'@',2)", "leadsource") }}
            as email_domain_type,

            -- keys
            masterrecordid as master_record_id,
            convertedaccountid as converted_account_id,
            convertedcontactid as converted_contact_id,
            convertedopportunityid as converted_opportunity_id,
            ownerid as owner_id,
            recordtypeid as record_type_id,
            round_robin_id__c as round_robin_id,
            instance_uuid__c as instance_uuid,
            lean_data_matched_account__c as lean_data_matched_account,

            -- lead info
            isconverted as is_converted,
            converteddate as converted_date,
            title as title,
            {{ it_job_title_hierarchy("title") }},
            donotcall as is_do_not_call,
            hasoptedoutofemail as has_opted_out_email,
            emailbounceddate as email_bounced_date,
            emailbouncedreason as email_bounced_reason,
            leadsource as lead_source,
            lead_from__c as lead_from,
            lead_source_type__c as lead_source_type,
            lead_conversion_approval_status__c as lead_conversiona_approval_status,
            street as street,
            city as city,
            state as state,
            statecode as state_code,
            country as country,
            countrycode as country_code,
            postalcode as postal_code,
            zi_company_country__c as zoominfo_company_country,
            zi_contact_country__c as zoominfo_contact_country,
            zi_company_state__c as zoominfo_company_state,
            zi_contact_state__c as zoominfo_contact_state,

            -- info
            requested_contact__c as requested_contact,
            company as company,
            dozisf__zoominfo_company_id__c as zoominfo_company_id,
            zi_company_revenue__c as zoominfo_company_revenue,
            zi_employee_count__c as zoominfo_company_employee_count,
            zi_company_city__c as zoominfo_company_city,
            zi_industry__c as zoominfo_company_industry,
            buying_process_for_procuring_gitlab__c as buying_process,
            core_check_in_notes__c as core_check_in_notes,
            industry as industry,
            largeaccount__c as is_large_account,
            outreach_stage__c as outreach_stage,
            sequence_step_number__c as outreach_step_number,
            interested_in_gitlab_ee__c as is_interested_gitlab_ee,
            interested_in_hosted_solution__c as is_interested_in_hosted,
            lead_assigned_datetime__c::timestamp as assigned_datetime,
            matched_account_top_list__c as matched_account_top_list,
            matched_account_owner_role__c as matched_account_owner_role,
            matched_account_sdr_assigned__c as matched_account_sdr_assigned,
            matched_account_gtm_strategy__c as matched_account_gtm_strategy,
            engagio__matched_account_type__c as matched_account_type,
            engagio__matched_account_owner_name__c
            as matched_account_account_owner_name,
            mql_date__c as marketo_qualified_lead_date,
            mql_datetime__c as marketo_qualified_lead_datetime,
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
            {{ sales_segment_cleaning("sales_segmentation__c") }} as sales_segmentation,
            mkto71_lead_score__c as person_score,
            status as lead_status,
            last_utm_campaign__c as last_utm_campaign,
            last_utm_content__c as last_utm_content,
            crm_partner_id_lookup__c as crm_partner_id,
            vartopiadrs__partner_prospect_acceptance__c as prospect_share_status,
            vartopiadrs__partner_prospect_status__c as partner_prospect_status,
            vartopiadrs__vartopia_prospect_id__c as partner_prospect_id,
            vartopiadrs__partner_prospect_owner_name__c as partner_prospect_owner_name,
            name_of_active_sequence__c as name_of_active_sequence,
            sequence_task_due_date__c::date as sequence_task_due_date,
            sequence_status__c as sequence_status,
            sequence_step_type2__c as sequence_step_type,
            actively_being_sequenced__c::boolean as is_actively_being_sequenced,

            {{ sfdc_source_buckets("leadsource") }}

            -- territory success planning info
            leandata_owner__c as tsp_owner,
            leandata_region__c as tsp_region,
            leandata_sub_region__c as tsp_sub_region,
            leandata_territory__c as tsp_territory,

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
            account_demographics_employee_count__c
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

            -- marketo sales insight
            mkto_si__last_interesting_moment_desc__c as marketo_last_interesting_moment,
            mkto_si__last_interesting_moment_date__c
            as marketo_last_interesting_moment_date,

            -- gitlab internal
            bdr_lu__c as business_development_look_up,
            business_development_rep_contact__c
            as business_development_representative_contact,
            business_development_representative__c
            as business_development_representative,
            sdr_lu__c as sales_development_representative,
            competition__c as competition,

            -- metadata
            createdbyid as created_by_id,
            createddate as created_date,
            isdeleted as is_deleted,
            lastactivitydate::date as last_activity_date,
            lastmodifiedbyid as last_modified_id,
            lastmodifieddate as last_modified_date,
            systemmodstamp

        from source

    )

select *
from renamed

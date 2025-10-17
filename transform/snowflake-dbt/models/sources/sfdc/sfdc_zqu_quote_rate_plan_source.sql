{{ config(tags=["mnpi"]) }}

with
    source as (select * from {{ source("salesforce", "zqu_quote_rate_plan") }}),
    renamed as (

        select

            -- ids
            id as zqu_quote_rate_plan_id,
            zqu__quoterateplanzuoraid__c as zqu_quote_rate_plan_zuora_id,
            name as zqu_quote_rate_plan_name,

            -- info
            one__c as one,
            admin_subtotal_test__c as admin_subtotal_test,
            admin_subtotal_summary__c as admin_subtotal_summary,
            charge_summary_sub_total__c as charge_summary_sub_total,
            license_amount__c as license_amount,
            professional_services_amount__c as professional_services_amount,
            true_up_amount__c as true_up_amount,
            ticket_group_numeric__c as ticket_group_numeric,
            zqu__quote__c as zqu_quote_id,
            zqu__quoteamendment__c as zqu_quote_amendment_id,
            zqu__amendmenttype__c as zqu_quote_amendment_type,
            zqu__productrateplan__c as zqu_product_rate_plan_id,
            zqu__productrateplanzuoraid__c as zqu_product_rate_plan_zuora_id,
            zqu__quoteproductname__c as zqu_quote_product_name,
            zqu__subscriptionrateplanzuoraid__c as zqu_subscription_rate_plan_zuora_id,
            rate_plan_charge_last_modified_time__c
            as rate_plan_charge_last_modified_time,
            zqu__time_product_added__c as zqu_time_product_added,

            -- metadata
            createdbyid as created_by_id,
            createddate as created_date,
            isdeleted as is_deleted,
            lastmodifiedbyid as last_modified_by_id,
            lastmodifieddate as last_modified_date,
            _sdc_received_at as sfdc_received_at,
            _sdc_extracted_at as sfdc_extracted_at,
            _sdc_table_version as sfdc_table_version,
            _sdc_batched_at as sfdc_batched_at,
            _sdc_sequence as sfdc_sequence,
            systemmodstamp as system_mod_stamp

        from source

    )

select *
from renamed

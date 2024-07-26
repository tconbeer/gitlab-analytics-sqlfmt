{{
    config(
        {
            "schema": "legacy",
            "database": env_var("SNOWFLAKE_PROD_DATABASE"),
        }
    )
}}

with
    source as (select * from {{ source("salesforce", "proof_of_concept") }}),
    renamed as (

        select
            -- keys
            id as pov_id,
            account__c as account_id,
            opportunity__c as opportunity_id,
            poc_owner__c as pov_owner_id,
            solutions_architect__c as solutions_architect_id,
            technical_account_manager__c as technical_account_manager_id,

            -- dates
            poc_start_date__c as pov_start_date,
            poc_close_date__c as pov_close_date,

            -- info
            name as pov_name,
            decline_reason__c as reason_for_decline,
            general_notes__c as general_notes,
            poc_length__c as pov_length,
            poc_milestone_in_collaboration_project__c as link_to_gitlab_milestone,
            poc_type__c as pov_type,
            result__c as pov_result,
            status__c as pov_status,
            success_criteria__c as success_criteria,
            unsuccessful_reason__c as unsuccessful_reason,

            -- metadata
            createdbyid as created_by_id,
            createddate as created_date,
            lastmodifiedbyid as last_modified_by_id,
            lastmodifieddate as last_modified_date,
            isdeleted::boolean as is_deleted

        from source

    )

select *
from renamed

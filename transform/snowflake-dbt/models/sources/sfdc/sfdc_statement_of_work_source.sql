with
    source as (select * from {{ source("salesforce", "statement_of_work") }}),
    renamed as (

        select
            -- keys
            id as ps_engagement_id,
            opportunity__c as opportunity_id,
            owner__c as owner_id,

            -- dates
            completed_date__c as completed_date,
            kick_off_date__c as kick_off_date,
            go_live_date__c as go_live_date,

            -- info
            name as ps_engagement_name,
            percentcomplete__c as percent_complete,
            signed_acceptance_from_customer__c as signed_acceptance_from_customer,
            status__c as status,

            -- metadata
            createdbyid as created_by_id,
            lastmodifiedbyid as last_modified_by_id,
            createddate as created_date,
            lastmodifieddate as last_modified_date,
            isdeleted as is_deleted

        from source

    )

select *
from renamed

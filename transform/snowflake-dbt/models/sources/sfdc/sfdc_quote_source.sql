with
    source as (select * from {{ source("salesforce", "quote") }}),
    renamed as (

        select
            -- keys
            id as quote_id,
            account_id__c as account_id,
            opportunity_id__c as opportunity_id,
            ownerid as owner_id,
            zqu__zuoraaccountid__c as zuora_account_id,
            zqu__zuorasubscriptionid__c as zuora_subscription_id,

            -- info
            zqu__startdate__c as contract_effective_date,
            createddate as created_date,
            zqu__primary__c as is_primary_quote,
            lastmodifieddate as last_modified_date,
            name as name,
            quote_tcv__c as quote_tcv,
            zqu__status__c as status,
            zqu__subscriptiontermstartdate__c as term_start_date,
            zqu__subscriptiontermenddate__c as term_end_date,
            systemmodstamp,
            isdeleted as is_deleted
        from source
    )

select *
from renamed

with
    source as (select * from {{ source("salesforce", "campaign_member") }}),
    renamed as (

        select
            id::varchar as campaign_member_id,

            -- keys
            campaignid::varchar as campaign_id,
            leadorcontactid::varchar as lead_or_contact_id,

            -- info
            type as campaign_member_type,
            hasresponded::boolean as campaign_member_has_responded,
            firstrespondeddate::date as campaign_member_response_date,
            mql_after_campaign__c::boolean as is_mql_after_campaign,

            -- metadata
            createddate::date as campaign_member_created_date,
            systemmodstamp,

            isdeleted as is_deleted

        from source
    )

select *
from renamed

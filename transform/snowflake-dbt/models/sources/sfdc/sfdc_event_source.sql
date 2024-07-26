with
    source as (select * from {{ source("salesforce", "event") }}),
    renamed as (

        select
            id as event_id,

            -- keys
            accountid::varchar as account_id,
            ownerid::varchar as owner_id,
            whoid::varchar as lead_or_contact_id,
            whatid::varchar as what_id,

            -- info      
            subject::varchar as event_subject,
            activitydate::date as event_date,
            activity_source__c::varchar as event_source,
            outreach_meeting_type__c::varchar as outreach_meeting_type,
            type::varchar as event_type,
            eventsubtype::varchar as event_sub_type,
            event_disposition__c::varchar as event_disposition,
            createddate::timestamp as created_at,

            isdeleted::boolean as is_deleted

        from source
    )

select *
from renamed

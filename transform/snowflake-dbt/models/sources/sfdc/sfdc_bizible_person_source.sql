with
    source as (select * from {{ source("salesforce", "bizible_person") }}),
    renamed as (

        select
            id as person_id,
            bizible2__lead__c as bizible_lead_id,
            bizible2__contact__c as bizible_contact_id,

            isdeleted::boolean as is_deleted

        from source
    )

select *
from renamed

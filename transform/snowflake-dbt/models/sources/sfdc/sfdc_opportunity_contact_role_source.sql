with
    source as (select * from {{ source("salesforce", "opportunity_contact_role") }}),
    renamed as (

        select

            -- keys
            id as opportunity_contact_role_id,
            contactid as contact_id,
            opportunityid as opportunity_id,
            createdbyid as created_by_id,
            lastmodifiedbyid as last_modified_by_id,

            -- info
            role as contact_role,
            isprimary as is_primary_contact,
            createddate as created_date,
            lastmodifieddate as last_modified_date

        from source
    )

select *
from renamed

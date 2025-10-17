-- depends_on: {{ ref('zuora_excluded_accounts') }}
with
    source as (select * from {{ source("snapshots", "zuora_contact_snapshots") }}),
    renamed as (

        select
            id as contact_id,
            -- keys
            accountid as account_id,

            -- contact info
            firstname as first_name,
            lastname as last_name,
            nickname,
            address1 as street_address,
            address2 as street_address2,
            county,
            state,
            postalcode as postal_code,
            city,
            country,
            taxregion as tax_region,
            workemail as work_email,
            workphone as work_phone,
            otherphone as other_phone,
            otherphonetype as other_phone_type,
            fax,
            homephone as home_phone,
            mobilephone as mobile_phone,
            personalemail as personal_email,
            description,

            -- metadata
            createdbyid as created_by_id,
            createddate as created_date,
            updatedbyid as updated_by_id,
            updateddate as updated_date,
            deleted as is_deleted,

            -- snapshot metadata
            dbt_scd_id,
            dbt_updated_at,
            dbt_valid_from,
            dbt_valid_to

        from source

    )

select *
from renamed

with
    source as (select * from {{ source("netsuite", "vendors") }}),
    renamed as (

        select
            -- Primary Key
            vendor_id::float as vendor_id,

            -- Foreign Key
            represents_subsidiary_id::float as subsidiary_id,
            currency_id::float as currency_id,

            -- Info
            companyname::varchar as vendor_name,
            openbalance::float as vendor_balance,
            comments::varchar as vendor_comments,

            -- Meta
            is1099eligible::boolean as is_1099_eligible,
            isinactive::boolean as is_inactive,
            is_person::boolean as is_person

        from source
        where lower(_fivetran_deleted) = 'false'

    )

select *
-- We no longer have first and last names for folks who are paid by contracts.
from renamed

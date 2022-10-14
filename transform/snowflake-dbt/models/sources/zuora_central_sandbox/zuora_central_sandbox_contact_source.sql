with
    source as (select * from {{ source("zuora_central_sandbox", "contact") }}),
    renamed as (

        select
            id as contact_id,
            -- keys
            account_id as account_id,


            -- contact info
            first_name as first_name,
            last_name as last_name,
            nick_name as nick_name,
            address_1 as street_address,
            address_2 as street_address2,
            county as county,
            state as state,
            postal_code as postal_code,
            city as city,
            country as country,
            tax_region as tax_region,
            work_email as work_email,
            work_phone as work_phone,
            other_phone as other_phone,
            other_phone_type as other_phone_type,
            fax as fax,
            home_phone as home_phone,
            mobile_phone as mobile_phone,
            personal_email as personal_email,
            description as description,


            -- metadata
            created_by_id as created_by_id,
            created_date as created_date,
            updated_by_id as updated_by_id,
            updated_date as updated_date,
            _fivetran_deleted as is_deleted

        from source

    )

select *
from renamed

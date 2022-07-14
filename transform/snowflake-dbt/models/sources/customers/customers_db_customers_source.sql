with
    source as (

        select *
        from {{ source("customers", "customers_db_customers") }}
        qualify row_number() OVER (partition by id order by updated_at desc) = 1

    ),
    renamed as (

        select distinct
            id::number as customer_id,
            first_name::varchar as customer_first_name,
            last_name::varchar as customer_last_name,
            email::varchar as customer_email,
            created_at::timestamp as customer_created_at,
            updated_at::timestamp as customer_updated_at,
            sign_in_count::number as sign_in_count,
            current_sign_in_at::timestamp as current_sign_in_at,
            last_sign_in_at::timestamp as last_sign_in_at,
            -- current_sign_in_ip,
            -- last_sign_in_ip,
            provider::varchar as customer_provider,
            nullif(uid, '')::varchar as customer_provider_user_id,
            zuora_account_id::varchar as zuora_account_id,
            country::varchar as country,
            state::varchar as state,
            city::varchar as city,
            vat_code::varchar as vat_code,
            company::varchar as company,
            company_size::varchar as company_size,
            salesforce_account_id::varchar as sfdc_account_id,
            billable::boolean as customer_is_billable,
            confirmed_at::timestamp as confirmed_at,
            confirmation_sent_at::timestamp as confirmation_sent_at
        from source

    )

select *
from renamed

{{ config({"alias": "customers_db_customers_snapshots"}) }}

with
    source as (

        select * from {{ source("snapshots", "customers_db_customers_snapshots") }}

    ),
    renamed as (

        select
            dbt_scd_id::varchar as customer_snapshot_id,
            id::number as customer_id,
            created_at::timestamp as customer_created_at,
            updated_at::timestamp as customer_updated_at,
            -- sign_in_count // missing from manifest, issue 1860,
            current_sign_in_at::timestamp as current_sign_in_at,
            last_sign_in_at::timestamp as last_sign_in_at,
            -- current_sign_in_ip,
            -- last_sign_in_ip,
            provider::varchar as customer_provider,
            nullif(uid, '')::varchar as customer_uid,
            zuora_account_id::varchar as zuora_account_id,
            country::varchar as country,
            state::varchar as state,
            city::varchar as city,
            company::varchar as company,
            salesforce_account_id::varchar as sfdc_account_id,
            billable::boolean as customer_is_billable,
            access_token::varchar as access_token,
            confirmation_token::varchar as confirmation_token,
            confirmed_at::timestamp as confirmed_at,
            confirmation_sent_at::timestamp as confirmation_sent_at,
            "DBT_VALID_FROM"::timestamp as valid_from,
            "DBT_VALID_TO"::timestamp as valid_to

        from source

    )

select *
from renamed

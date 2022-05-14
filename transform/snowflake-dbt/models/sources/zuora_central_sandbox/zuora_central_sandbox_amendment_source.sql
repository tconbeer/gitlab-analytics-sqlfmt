with
    source as (select * from {{ source("zuora_central_sandbox", "amendment") }}),
    renamed as (

        select
            -- keys
            id::varchar as amendment_id,
            subscription_id::varchar as subscription_id,
            code::varchar as amendment_code,
            status::varchar as amendment_status,
            name::varchar as amendment_name,
            service_activation_date::timestamp_tz as service_activation_date,
            current_term::number as current_term,
            description::varchar as amendment_description,
            current_term_period_type::varchar as current_term_period_type,
            customer_acceptance_date::timestamp_tz as customer_acceptance_date,
            effective_date::timestamp_tz as effective_date,
            renewal_setting::varchar as renewal_setting,
            term_start_date::timestamp_tz as term_start_date,
            contract_effective_date::timestamp_tz as contract_effective_date,
            type::varchar as amendment_type,
            auto_renew::boolean as auto_renew,
            renewal_term_period_type::varchar as renewal_term_period_type,
            renewal_term::number as renewal_term,
            term_type::varchar as term_type,

            -- metadata
            created_by_id::varchar as created_by_id,
            created_date::timestamp_tz as created_date,
            updated_by_id::varchar as updated_by_id,
            updated_date::timestamp_tz as updated_date,
            _fivetran_deleted::boolean as is_deleted,
            _fivetran_synced::timestamp_tz as _fivetran_synced

        from source
    )

select *
from renamed

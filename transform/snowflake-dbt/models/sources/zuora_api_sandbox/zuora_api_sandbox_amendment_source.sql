with
    source as (select * from {{ source("zuora_api_sandbox", "amendment") }}),
    renamed as (

        select
            -- keys
            id::varchar as amendment_id,
            subscriptionid::varchar as subscription_id,
            code::varchar as amendment_code,
            status::varchar as amendment_status,
            name::varchar as amendment_name,
            serviceactivationdate::timestamp_tz as service_activation_date,
            currentterm::number as current_term,
            description::varchar as amendment_description,
            currenttermperiodtype::varchar as current_term_period_type,
            customeracceptancedate::timestamp_tz as customer_acceptance_date,
            effectivedate::timestamp_tz as effective_date,
            renewalsetting::varchar as renewal_setting,
            termstartdate::timestamp_tz as term_start_date,
            contracteffectivedate::timestamp_tz as contract_effective_date,
            type::varchar as amendment_type,
            autorenew::boolean as auto_renew,
            renewaltermperiodtype::varchar as renewal_term_period_type,
            renewalterm::number as renewal_term,
            termtype::varchar as term_type,

            -- metadata
            createdbyid::varchar as created_by_id,
            createddate::timestamp_tz as created_date,
            updatedbyid::varchar as updated_by_id,
            updateddate::timestamp_tz as updated_date,
            deleted::boolean as is_deleted,
            _sdc_table_version::number as sdc_table_version,
            _sdc_received_at::timestamp_tz as sdc_received_at,
            _sdc_sequence::number as sdc_sequence,
            _sdc_batched_at::timestamp_tz as sdc_batched_at,
            _sdc_extracted_at::timestamp_tz as sdc_extracted_at

        from source
    )

select *
from renamed

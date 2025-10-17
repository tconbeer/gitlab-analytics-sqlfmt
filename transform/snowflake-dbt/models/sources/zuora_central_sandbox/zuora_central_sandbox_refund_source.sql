with
    source as (select * from {{ source("zuora_central_sandbox", "refund") }}),
    renamed as (

        select
            refund_number::varchar as refund_number,
            id::varchar as refund_id,

            -- Foreign Keys
            account_id::varchar as account_id,
            -- parent_account_id::VARCHAR                  AS parent_account_id,
            -- Info
            accounting_code::varchar as accounting_code,
            amount::float as refund_amount,
            bill_to_contact_id::varchar as bill_to_contact_id,
            comment::varchar as comment,
            created_by_id::varchar as created_by_id,
            created_date::timestamp_tz as created_date,
            default_payment_method_id::varchar as default_payment_method_id,
            gateway::varchar as gateway,
            gateway_response::varchar as gateway_response,
            gateway_response_code::varchar as gateway_response_code,
            gateway_state::varchar as gateway_state,
            method_type::varchar as method_type,
            payment_method_id::varchar as payment_method_id,
            payment_method_snapshot_id::varchar as payment_method_snapshot_id,
            reason_code::varchar as reason_code,
            reference_id::varchar as reference_id,
            refund_date::timestamp_tz as refund_date,
            refund_transaction_time::timestamp_tz as refund_transaction_time,
            second_refund_reference_id::varchar as second_refund_reference_id,
            soft_descriptor::varchar as soft_descriptor,
            soft_descriptor_phone::varchar as soft_descriptor_phone,
            sold_to_contact_id::varchar as sold_to_contact_id,
            source_type::varchar as source_type,
            status::varchar as refund_status,
            submitted_on::timestamp_tz as submitted_on,
            transferred_to_accounting::varchar as transferred_to_accounting,
            type::varchar as refund_type,
            updated_by_id::varchar as updated_by_id,
            updated_date::timestamp_tz as updated_date,
            _fivetran_deleted::boolean as is_deleted

        from source

    )

select *
from renamed

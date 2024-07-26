-- depends_on: {{ ref('zuora_excluded_accounts') }}
with
    source as (select * from {{ source("snapshots", "zuora_refund_snapshots") }}),
    renamed as (

        select
            -- Primary Keys
            refundnumber::varchar as refund_number,
            id::varchar as refund_id,

            -- Foreign Keys
            accountid::varchar as account_id,
            parentaccountid::varchar as parent_account_id,

            -- Info
            accountingcode::varchar as accounting_code,
            amount::float as refund_amount,
            billtocontactid::varchar as bill_to_contact_id,
            cancelledon::timestamp_tz as cancelled_on,
            comment::varchar as comment,
            createdbyid::varchar as created_by_id,
            createddate::timestamp_tz as created_date,
            defaultpaymentmethodid::varchar as default_payment_method_id,
            gateway::varchar as gateway,
            gatewayresponse::varchar as gateway_response,
            gatewayresponsecode::varchar as gateway_response_code,
            gatewaystate::varchar as gateway_state,
            methodtype::varchar as method_type,
            paymentmethodid::varchar as payment_method_id,
            paymentmethodsnapshotid::varchar as payment_method_snapshot_id,
            reasoncode::varchar as reason_code,
            referenceid::varchar as reference_id,
            refunddate::timestamp_tz as refund_date,
            refundtransactiontime::timestamp_tz as refund_transaction_time,
            secondrefundreferenceid::varchar as second_refund_reference_id,
            softdescriptor::varchar as soft_descriptor,
            softdescriptorphone::varchar as soft_descriptor_phone,
            soldtocontactid::varchar as sold_to_contact_id,
            sourcetype::varchar as source_type,
            status::varchar as refund_status,
            submittedon::timestamp_tz as submitted_on,
            transferredtoaccounting::varchar as transferred_to_accounting,
            type::varchar as refund_type,
            updatedbyid::varchar as updated_by_id,
            updateddate::timestamp_tz as updated_date,
            deleted::boolean as is_deleted,

            -- snapshot metadata
            dbt_scd_id,
            dbt_updated_at,
            dbt_valid_from,
            dbt_valid_to

        from source

    )

select *
from renamed

with
    source as (select * from {{ source("zuora_central_sandbox", "invoice_payment") }}),
    renamed as (

        select
            -- keys
            id::varchar as invoice_payment_id,
            invoice_id::varchar as invoice_id,
            account_id::varchar as account_id,
            accounting_period_id::varchar as accounting_period_id,

            -- info
            bill_to_contact_id::varchar as bill_to_contact_id,
            cash_accounting_code_id::varchar as cash_accounting_code_id,
            default_payment_method_id::varchar as default_payment_method_id,
            journal_entry_id::varchar as journal_entry_id,
            journal_run_id::varchar as journal_run_id,
            -- parent_account_id::VARCHAR               AS parent_account_id,
            payment_id::varchar as payment_id,
            payment_method_id::varchar as payment_method_id,
            payment_method_snapshot_id::varchar as payment_method_snapshot_id,
            sold_to_contact_id::varchar as sold_to_contact_id,

            -- financial info
            amount::float as payment_amount,
            refund_amount::float as refund_amount,

            -- metadata
            updated_by_id::varchar as updated_by_id,
            updated_date::timestamp_tz as updated_date,
            _fivetran_deleted::boolean as is_deleted

        from source

    )

select *
from renamed

with
    source as (select * from {{ source("zuora_api_sandbox", "invoice_payment") }}),
    renamed as (

        select
            -- keys
            id::varchar as invoice_payment_id,
            invoiceid::varchar as invoice_id,
            accountid::varchar as account_id,
            accountingperiodid::varchar as accounting_period_id,

            -- info
            billtocontactid::varchar as bill_to_contact_id,
            cashaccountingcodeid::varchar as cash_accounting_code_id,
            defaultpaymentmethodid::varchar as default_payment_method_id,
            journalentryid::varchar as journal_entry_id,
            journalrunid::varchar as journal_run_id,
            parentaccountid::varchar as parent_account_id,
            paymentid::varchar as payment_id,
            paymentmethodid::varchar as payment_method_id,
            paymentmethodsnapshotid::varchar as payment_method_snapshot_id,
            soldtocontactid::varchar as sold_to_contact_id,

            -- financial info
            amount::float as payment_amount,
            refundamount::float as refund_amount,

            -- metadata
            updatedbyid::varchar as updated_by_id,
            updateddate::timestamp_tz as updated_date,
            deleted::boolean as is_deleted

        from source

    )

select *
from renamed

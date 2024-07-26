with
    source as (select * from {{ source("zuora_central_sandbox", "invoice") }}),
    renamed as (

        select
            id as invoice_id,
            -- keys
            account_id as account_id,

            -- invoice metadata
            due_date as due_date,
            invoice_number as invoice_number,
            invoice_date as invoice_date,
            status as status,

            last_email_sent_date as last_email_sent_date,
            posted_date as posted_date,
            target_date as target_date,

            includes_one_time as includes_one_time,
            includes_recurring as includes_recurring,
            includes_usage as includes_usage,
            transferred_to_accounting as transferred_to_accounting,

            -- financial info
            adjustment_amount as adjustment_amount,
            amount as amount,
            amount_without_tax as amount_without_tax,
            balance as balance,
            credit_balance_adjustment_amount as credit_balance_adjustment_amount,
            payment_amount as payment_amount,
            refund_amount as refund_amount,
            tax_amount as tax_amount,
            tax_exempt_amount as tax_exempt_amount,
            comments as comments,

            -- metadata
            created_by_id as created_by_id,
            created_date as created_date,
            posted_by as posted_by,
            source as source,
            source as source_id,
            updated_by_id as updated_by_id,
            updated_date as updated_date,
            _fivetran_deleted as is_deleted

        from source

    )

select *
from renamed

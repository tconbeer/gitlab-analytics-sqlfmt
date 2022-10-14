{{ config(tags=["mnpi"]) }}

with
    source as (select * from {{ source("netsuite", "transactions") }}),
    renamed as (

        select
            -- Primary Key
            transaction_id::float as transaction_id,

            -- Info
            entity_id::float as entity_id,
            accounting_period_id::float as accounting_period_id,
            currency_id::float as currency_id,
            transaction_type::varchar as transaction_type,
            external_ref_number::varchar as external_ref_number,
            transaction_extid::varchar as transaction_ext_id,
            transaction_number::varchar as transaction_number,
            memo::varchar as memo,
            tranid::varchar as document_id,
            opening_balance_transaction::varchar as balance,
            exchange_rate::float as exchange_rate,
            weighted_total::float as total,
            status::varchar as status,
            due_date::timestamp_tz as due_date,
            trandate::timestamp_tz as transaction_date,
            sales_effective_date::timestamp_tz as sales_effective_date

        from source

    )

select *
from renamed

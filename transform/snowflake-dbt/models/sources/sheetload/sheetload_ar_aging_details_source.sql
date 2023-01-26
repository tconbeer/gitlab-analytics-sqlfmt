with
    source as (select * from {{ source("sheetload", "ar_aging_details") }}),
    renamed as (

        select
            invoice_period::date as invoice_period,
            account_name::varchar as account_name,
            account_number::varchar as account_number,
            account_entity::varchar as account_entity,
            currency::varchar as currency,
            home_currency::varchar as home_currency,
            exchange_rate_date::date as exchange_rate_date,
            exchange_rate::number as exchange_rate,
            invoice_number::varchar as invoice_number,
            invoice_date::date as invoice_date,
            regexp_replace(invoice_amount, '[(),]', '')::number as invoice_amount,
            due_date::date as due_date,
            days_aging::number as days_aging,
            aging_bucket::varchar as aging_bucket,
            regexp_replace(invoice_balance, '[(),]', '')::number as invoice_balance,
            invoice_balance_home_currency::number as invoice_balance_home_currency,
            invoice_balance_currency_rounding::number
            as invoice_balance_currency_rounding,
            "current"::number as "current",
            "1_to_30_days_past_due"::number as "1_to_30_days_past_due",
            "31_to_60_days_past_due"::number as "31_to_60_days_past_due",
            "61_to_90_days_past_due"::number as "61_to_90_days_past_due",
            "91_to_120_days_past_due"::number as "91_to_120_days_past_due",
            more_than_120_days_past_due::number as more_than_120_days_past_due,
            current_home_currency::number as current_home_currency,
            "1_to_30_days_past_due_home_currency"::number
            as "1_to_30_days_past_due_home_currency",
            "31_to_60_days_past_due_home_currency"::number
            as "31_to_60_days_past_due_home_currency",
            "61_to_90_days_past_due_home_currency"::number
            as "61_to_90_days_past_due_home_currency",
            "91_to_120_days_past_due_home_currency"::number
            as "91_to_120_days_past_due_home_currency",
            more_than_120_days_past_due_home_currency::number
            as more_than_120_days_past_due_home_currency
        from source

    )

select *
from renamed

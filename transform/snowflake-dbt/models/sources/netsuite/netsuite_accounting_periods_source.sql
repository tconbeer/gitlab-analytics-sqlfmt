with
    source as (select * from {{ source("netsuite", "accounting_periods") }}),
    renamed as (

        select
            {{ dbt_utils.surrogate_key(["accounting_period_id", "full_name"]) }}
            as accounting_period_unique_id,
            -- Primary Key
            accounting_period_id::float as accounting_period_id,

            -- Foreign Keys
            parent_id::float as parent_id,
            year_id::float as year_id,

            -- Info
            name::varchar as accounting_period_name,
            full_name::varchar as accounting_period_full_name,
            fiscal_calendar_id::float as fiscal_calendar_id,
            closed_on::timestamp_tz as accounting_period_close_date,
            ending::timestamp_tz as accounting_period_end_date,
            starting::timestamp_tz as accounting_period_starting_date,

            -- Meta
            locked_accounts_payable::boolean as is_accounts_payable_locked,
            locked_accounts_receivable::boolean as is_accounts_receivables_locked,
            locked_all::boolean as is_all_locked,
            locked_payroll::boolean as is_payroll_locked,
            closed::boolean as is_accouting_period_closed,
            closed_accounts_payable::boolean as is_accounts_payable_closed,
            closed_accounts_receivable::boolean as is_accounts_receivables_closed,
            closed_all::boolean as is_all_closed,
            closed_payroll::boolean as is_payroll_closed,
            isinactive::boolean as is_accounting_period_inactive,
            is_adjustment::boolean as is_accounting_period_adjustment,
            quarter::boolean as is_quarter,
            year_0::boolean as is_year

        from source

    )

select *
from renamed

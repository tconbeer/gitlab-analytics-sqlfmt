with
    source as (select * from {{ source("netsuite", "accounts") }}),
    renamed as (

        select
            -- Primary Key
            account_id::float as account_id,

            -- Foreign Keys
            parent_id::float as parent_account_id,
            currency_id::float as currency_id,
            department_id::float as department_id,

            -- Info
            name::varchar as account_name,
            full_name::varchar as account_full_name,
            full_description::varchar as account_full_description,
            accountnumber::varchar as account_number,
            expense_type_id::float as expense_type_id,
            type_name::varchar as account_type,
            type_sequence::float as account_type_sequence,
            openbalance::float as current_account_balance,
            cashflow_rate_type::varchar as cashflow_rate_type,
            general_rate_type::varchar as general_rate_type,

            -- Meta
            isinactive::boolean as is_account_inactive,
            is_balancesheet::boolean as is_balancesheet_account,
            is_included_in_elimination::boolean as is_account_included_in_elimination,
            is_included_in_reval::boolean as is_account_included_in_reval,
            is_including_child_subs::boolean
            as is_account_including_child_subscriptions,
            is_leftside::boolean as is_leftside_account,
            is_summary::boolean as is_summary_account

        from source

    )

select *
from renamed

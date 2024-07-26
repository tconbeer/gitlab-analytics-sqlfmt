with
    base_accounts as (

        select *
        from {{ ref("netsuite_accounts_source") }}
        where account_number is not null

    ),
    ultimate_account as (

        select
            a.account_id,
            a.account_number,
            case
                when a.parent_account_id is not null
                then a.parent_account_id
                else a.account_id
            end as parent_account_id,
            case
                when b.account_number is not null
                then b.account_number
                else a.account_number
            end as parent_account_number,
            case
                when b.account_number is not null
                then b.account_number || ' - ' || a.account_number
                else a.account_number
            end as unique_account_number,
            a.currency_id,
            a.account_name,
            a.account_full_name,
            a.account_full_description,
            a.account_type,
            a.general_rate_type,
            a.is_account_inactive,
            a.is_balancesheet_account,
            a.is_leftside_account
        from base_accounts a
        left join base_accounts b on a.parent_account_id = b.account_id

    )

select *
from ultimate_account

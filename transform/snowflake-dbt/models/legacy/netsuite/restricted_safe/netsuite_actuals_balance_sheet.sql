{% set net_income_retained_earnings = (
    "income",
    "other income",
    "expense",
    "other expense",
    "other income",
    "cost of goods sold",
) %}

with
    accounts as (select * from {{ ref("netsuite_accounts_xf") }}),
    accounting_books as (select * from {{ ref("netsuite_accounting_books") }}),
    accounting_periods as (select * from {{ ref("netsuite_accounting_periods") }}),
    classes as (select * from {{ ref("netsuite_classes") }}),
    consolidated_exchange_rates as (

        select * from {{ ref("netsuite_consolidated_exchange_rates") }}

    ),
    date_details as (

        select distinct
            first_day_of_month, fiscal_year, fiscal_quarter, fiscal_quarter_name
        from {{ ref("date_details") }}

    ),
    departments as (select * from {{ ref("netsuite_departments_xf") }}),
    subsidiaries as (select * from {{ ref("netsuite_subsidiaries_source") }}),
    transactions as (select * from {{ ref("netsuite_transactions_source") }}),
    -- exchange rates used, by accounting period, to convert to parent subsidiary
    transaction_lines as (select * from {{ ref("netsuite_transaction_lines_xf") }}),
    period_exchange_rate_map as (

        select
            consolidated_exchange_rates.accounting_period_id,
            consolidated_exchange_rates.average_rate,
            consolidated_exchange_rates.current_rate,
            consolidated_exchange_rates.historical_rate,
            consolidated_exchange_rates.from_subsidiary_id,
            consolidated_exchange_rates.to_subsidiary_id
        from consolidated_exchange_rates
        where
            consolidated_exchange_rates.to_subsidiary_id
            -- constrait - only the primary subsidiary has no parent
            in (select subsidiary_id from subsidiaries where parent_id is null)
            and consolidated_exchange_rates.accounting_book_id
            in (select accounting_book_id from accounting_books where is_primary = true)

    ),  -- account table with exchange rate details by accounting period
    account_period_exchange_rate_map as (

        select
            period_exchange_rate_map.accounting_period_id,
            period_exchange_rate_map.from_subsidiary_id,
            period_exchange_rate_map.to_subsidiary_id,
            accounts.account_id,
            case
                when lower(accounts.general_rate_type) = 'historical'
                then period_exchange_rate_map.historical_rate
                when lower(accounts.general_rate_type) = 'current'
                then period_exchange_rate_map.current_rate
                when lower(accounts.general_rate_type) = 'average'
                then period_exchange_rate_map.average_rate
                else null
            end as exchange_rate
        from accounts
        cross join period_exchange_rate_map

    ),  -- transaction line totals, by accounts, accounting period and subsidiary
    transaction_lines_w_accounting_period as (

        select
            transaction_lines.transaction_id,
            transaction_lines.transaction_line_id,
            transaction_lines.memo,
            transaction_lines.entity_name,
            transactions.document_id,
            transactions.transaction_type,
            transaction_lines.subsidiary_id,
            transaction_lines.account_id,
            transaction_lines.class_id,
            transaction_lines.department_id,
            transactions.accounting_period_id as transaction_accounting_period_id,
            coalesce(transaction_lines.amount, 0) as unconverted_amount
        from transaction_lines
        inner join
            transactions
            on transaction_lines.transaction_id = transactions.transaction_id
        where lower(transactions.transaction_type) != 'revenue arrangement'

    -- period ids with all future period ids.  this is needed to calculate cumulative
    -- totals by correct exchange rates.
    ),
    period_id_list_to_current_period as (

        select
            base.accounting_period_id,
            array_agg(multiplier.accounting_period_id) within group (
                order by multiplier.accounting_period_id
            ) as accounting_periods_to_include_for
        from accounting_periods as base
        inner join
            accounting_periods as multiplier
            on base.accounting_period_starting_date
            <= multiplier.accounting_period_starting_date
            and base.is_quarter = multiplier.is_quarter
            and base.is_year = multiplier.is_year
            and base.fiscal_calendar_id = multiplier.fiscal_calendar_id
            and multiplier.accounting_period_starting_date <= current_timestamp()
        where
            base.is_quarter = false
            and base.is_year = false
            and base.fiscal_calendar_id
            -- fiscal calendar will align with parent subsidiary's default calendar
            = (select fiscal_calendar_id from subsidiaries where parent_id is null)
            {{ dbt_utils.group_by(n=1) }}

    ),
    flatten_period_id_array as (

        select
            accounting_period_id,
            reporting_accounting_period_id.value as reporting_accounting_period_id
        from
            period_id_list_to_current_period,
            lateral flatten(
                input => accounting_periods_to_include_for
            ) reporting_accounting_period_id
        where array_size(accounting_periods_to_include_for) > 1

    ),
    transactions_in_every_calculation_period as (

        select transaction_lines_w_accounting_period.*, reporting_accounting_period_id
        from transaction_lines_w_accounting_period
        inner join
            flatten_period_id_array
            on flatten_period_id_array.accounting_period_id
            = transaction_lines_w_accounting_period.transaction_accounting_period_id

    ),
    transactions_in_every_calculation_period_w_exchange_rates as (

        select
            transactions_in_every_calculation_period.*,
            exchange_reporting_period.exchange_rate as exchange_reporting_period,
            exchange_transaction_period.exchange_rate as exchange_transaction_period
        from transactions_in_every_calculation_period
        left join
            account_period_exchange_rate_map as exchange_reporting_period
            on transactions_in_every_calculation_period.account_id
            = exchange_reporting_period.account_id
            and transactions_in_every_calculation_period.reporting_accounting_period_id
            = exchange_reporting_period.accounting_period_id
            and transactions_in_every_calculation_period.subsidiary_id
            = exchange_reporting_period.from_subsidiary_id
        left join
            account_period_exchange_rate_map as exchange_transaction_period
            on transactions_in_every_calculation_period.account_id
            = exchange_transaction_period.account_id
            and transactions_in_every_calculation_period.transaction_accounting_period_id
            = exchange_transaction_period.accounting_period_id
            and transactions_in_every_calculation_period.subsidiary_id
            = exchange_transaction_period.from_subsidiary_id

    ),
    transactions_with_converted_amounts as (

        select
            transactions_in_every_calculation_period_w_exchange_rates.*,
            unconverted_amount
            * exchange_transaction_period
            as converted_amount_using_transaction_accounting_period,
            unconverted_amount
            * exchange_reporting_period as converted_amount_using_reporting_month
        from transactions_in_every_calculation_period_w_exchange_rates

    ),
    balance_sheet as (

        select
            transactions_with_converted_amounts.document_id,
            transactions_with_converted_amounts.memo,
            transactions_with_converted_amounts.entity_name,
            departments.parent_department_name,
            departments.department_name,
            classes.class_name,
            transactions_with_converted_amounts.transaction_type,
            reporting_accounting_periods.accounting_period_id,
            reporting_accounting_periods.accounting_period_starting_date::date
            as accounting_period,
            reporting_accounting_periods.accounting_period_name,
            accounts.is_account_inactive,
            case
                when
                    (
                        lower(accounts.account_type)
                        in {{ net_income_retained_earnings }}
                        and reporting_accounting_periods.year_id
                        = transaction_accounting_periods.year_id
                    )
                then 'net income'
                when lower(accounts.account_type) in {{ net_income_retained_earnings }}
                then 'retained earnings'
                when accounts.account_number = '3000'
                then 'retained earnings'
                else lower(accounts.account_name)
            end as account_name,
            case
                when
                    (
                        lower(accounts.account_type)
                        in {{ net_income_retained_earnings }}
                        and reporting_accounting_periods.year_id
                        = transaction_accounting_periods.year_id
                    )
                then 'net income'
                when lower(accounts.account_type) in {{ net_income_retained_earnings }}
                then 'retained earnings'
                when accounts.account_number = '3000'
                then 'retained earnings'
                when accounts.account_number = '1351'
                then 'other current asset'
                else lower(accounts.account_type)
            end as account_type,
            case
                when lower(accounts.account_type) in {{ net_income_retained_earnings }}
                then null
                when accounts.account_number = '3000'
                then null
                else accounts.account_id
            end as account_id,
            case
                when lower(accounts.account_type) in {{ net_income_retained_earnings }}
                then ''
                when accounts.account_number = '3000'
                then ''
                else accounts.account_number
            end as account_number,
            case
                when lower(accounts.account_type) in {{ net_income_retained_earnings }}
                then ''
                when accounts.account_number = '3000'
                then ''
                else accounts.unique_account_number
            end as unique_account_number,
            sum(
                case
                    when
                        lower(accounts.account_type)
                        in {{ net_income_retained_earnings }}
                    then - converted_amount_using_transaction_accounting_period
                    when accounts.account_number = '3000'
                    then - converted_amount_using_transaction_accounting_period
                    when
                        (
                            lower(accounts.general_rate_type) = 'historical'
                            and accounts.is_leftside_account = false
                        )
                    then - converted_amount_using_transaction_accounting_period
                    when
                        (
                            lower(accounts.general_rate_type) = 'historical'
                            and accounts.is_leftside_account = true
                        )
                    then converted_amount_using_transaction_accounting_period
                    when
                        (
                            accounts.is_balancesheet_account = true
                            and accounts.is_leftside_account = false
                        )
                    then - converted_amount_using_reporting_month
                    when
                        (
                            accounts.is_balancesheet_account = true
                            and accounts.is_leftside_account = true
                        )
                    then converted_amount_using_reporting_month
                    else 0
                end
            ) as actual_amount
        from transactions_with_converted_amounts
        left join
            accounts
            on transactions_with_converted_amounts.account_id = accounts.account_id
        left join
            classes on transactions_with_converted_amounts.class_id = classes.class_id
        left join
            departments
            on transactions_with_converted_amounts.department_id
            = departments.department_id
        left join
            accounting_periods as reporting_accounting_periods
            on transactions_with_converted_amounts.reporting_accounting_period_id
            = reporting_accounting_periods.accounting_period_id
        left join
            accounting_periods as transaction_accounting_periods
            on transactions_with_converted_amounts.transaction_accounting_period_id
            = transaction_accounting_periods.accounting_period_id
        where
            reporting_accounting_periods.fiscal_calendar_id
            = (select fiscal_calendar_id from subsidiaries where parent_id is null)
            and transaction_accounting_periods.fiscal_calendar_id
            = (select fiscal_calendar_id from subsidiaries where parent_id is null)
            and lower(accounts.account_type) != 'statistical'
            and accounts.account_number != '3035'
            {{ dbt_utils.group_by(n=16) }}

        union all

        select
            transactions_with_converted_amounts.document_id,
            transactions_with_converted_amounts.memo,
            transactions_with_converted_amounts.entity_name,
            departments.parent_department_name,
            departments.department_name,
            classes.class_name,
            transactions_with_converted_amounts.transaction_type,
            reporting_accounting_periods.accounting_period_id,
            reporting_accounting_periods.accounting_period_starting_date::date
            as accounting_period,
            reporting_accounting_periods.accounting_period_name,
            accounts.is_account_inactive,
            'Cumulative Translation Adjustment' as account_name,
            'Cumulative Translation Adjustment' as account_type,
            null as account_id,
            '' as account_number,
            null as unique_account_number,
            sum(
                case
                    when lower(account_type) in {{ net_income_retained_earnings }}
                    then converted_amount_using_transaction_accounting_period
                    when
                        lower(account_type)
                        in ('equity', 'retained earnings', 'net income')
                    then converted_amount_using_transaction_accounting_period
                    else converted_amount_using_reporting_month
                end
            ) as actual_amount
        from transactions_with_converted_amounts
        left join
            accounts
            on transactions_with_converted_amounts.account_id = accounts.account_id
        left join
            classes on transactions_with_converted_amounts.class_id = classes.class_id
        left join
            departments
            on transactions_with_converted_amounts.department_id
            = departments.department_id
        left join
            accounting_periods as reporting_accounting_periods
            on transactions_with_converted_amounts.reporting_accounting_period_id
            = reporting_accounting_periods.accounting_period_id
        left join
            accounting_periods as transaction_accounting_periods
            on transactions_with_converted_amounts.transaction_accounting_period_id
            = transaction_accounting_periods.accounting_period_id
        where
            reporting_accounting_periods.fiscal_calendar_id
            = (select fiscal_calendar_id from subsidiaries where parent_id is null)
            and transaction_accounting_periods.fiscal_calendar_id
            = (select fiscal_calendar_id from subsidiaries where parent_id is null)
            and lower(accounts.account_type) != 'statistical'
            and accounts.account_number != '3035'
            {{ dbt_utils.group_by(n=11) }}

    ),
    balance_sheet_grouping as (

        select
            document_id,
            memo,
            entity_name,
            transaction_type,
            account_id,
            account_name,
            account_number,
            unique_account_number,
            account_number || ' - ' || account_name as unique_account_name,
            account_type,
            parent_department_name,
            department_name,
            class_name,
            case
                when
                    account_type in (
                        'accounts receivable',
                        'bank',
                        'other current asset',
                        'unbilled receivable',
                        'deferred expense'
                    )
                then '1-current assets'
                when
                    account_type in (
                        'accounts payable',
                        'credit card',
                        'deferred revenue',
                        'other current liability'
                    )
                then '1-current liabilities'
                when account_type in ('fixed asset')
                then '3-fixed assets'
                when account_type in ('long term liability')
                then '2-long term liabilities'
                when account_type in ('other asset')
                then '2-other assets'
                when
                    account_type in (
                        'net income',
                        'retained earnings',
                        'equity',
                        'Cumulative Translation Adjustment'
                    )
                then '3-equity'
                else 'need classification'
            end as balance_sheet_grouping_level_2,
            case
                when
                    account_type in (
                        'accounts receivable',
                        'bank',
                        'other current asset',
                        'unbilled receivable',
                        'fixed asset',
                        'other asset',
                        'deferred expense'
                    )
                then '1-assets'
                when
                    account_type in (
                        'accounts payable',
                        'credit card',
                        'deferred revenue',
                        'other current liability',
                        'equity',
                        'long term liability',
                        'net income',
                        'retained earnings',
                        'Cumulative Translation Adjustment'
                    )
                then '2-liabilities & equity'
                else 'need classification'
            end as balance_sheet_grouping_level_3,
            is_account_inactive,
            actual_amount,
            accounting_period_id,
            accounting_period,
            accounting_period_name,
            fiscal_year,
            fiscal_quarter,
            fiscal_quarter_name

        from balance_sheet b
        left join date_details d on b.accounting_period = d.first_day_of_month

    )

select *
from balance_sheet_grouping
order by accounting_period, account_name

with
    transactions as (select * from {{ ref("netsuite_transactions_source") }}),
    transaction_lines as (select * from {{ ref("netsuite_transaction_lines_xf") }}),
    accounting_periods as (select * from {{ ref("netsuite_accounting_periods") }}),
    accounts as (select * from {{ ref("netsuite_accounts_xf") }}),
    classes as (select * from {{ ref("netsuite_classes") }}),
    subsidiaries as (select * from {{ ref("netsuite_subsidiaries_source") }}),
    departments as (select * from {{ ref("netsuite_departments_xf") }}),
    consolidated_exchange_rates as (

        select * from {{ ref("netsuite_consolidated_exchange_rates") }}

    ),
    date_details as (

        select distinct
            first_day_of_month, fiscal_year, fiscal_quarter, fiscal_quarter_name
        from {{ ref("date_details") }}

    ),
    cost_category as (select * from {{ ref("netsuite_expense_cost_category") }}),
    opex_cogs as (

        select
            t.transaction_id,
            t.external_ref_number,
            t.transaction_ext_id,
            t.document_id,
            tl.memo as transaction_lines_memo,
            tl.entity_name,
            tl.receipt_url,
            t.status,
            t.transaction_type,
            a.account_id,
            a.account_name,
            a.account_full_name,
            a.account_number,
            a.unique_account_number,
            a.parent_account_number,
            cl.class_id,
            cl.class_name,
            d.department_id,
            d.department_name,
            d.parent_department_name,
            ap.accounting_period_id,
            ap.accounting_period_starting_date::date as accounting_period,
            ap.accounting_period_name,
            sum(
                case
                    when tl.subsidiary_id = 1
                    then amount
                    else (tl.amount * e.average_rate)
                end
            ) as actual_amount
        from transaction_lines tl
        left join transactions t on tl.transaction_id = t.transaction_id
        left join accounts a on a.account_id = tl.account_id
        left join classes cl on tl.class_id = cl.class_id
        left join departments d on d.department_id = tl.department_id
        left join
            accounting_periods ap on ap.accounting_period_id = t.accounting_period_id
        left join subsidiaries s on tl.subsidiary_id = s.subsidiary_id
        left join
            consolidated_exchange_rates e
            on ap.accounting_period_id = e.accounting_period_id
            and e.from_subsidiary_id = s.subsidiary_id
        where
            a.account_number between '5000' and '6999'
            and ap.fiscal_calendar_id = 2
            and e.to_subsidiary_id = 1
            {{ dbt_utils.group_by(n=23) }}

    ),
    income_statement_grouping as (

        select
            oc.transaction_id,
            oc.external_ref_number,
            oc.transaction_ext_id,
            oc.document_id,
            oc.account_id,
            oc.account_name,
            oc.account_full_name,
            oc.account_number || ' - ' || oc.account_name as unique_account_name,
            oc.account_number,
            oc.parent_account_number,
            oc.unique_account_number,
            oc.actual_amount,
            case
                when oc.account_number between '5000' and '5999'
                then '2-cost of sales'
                when oc.account_number between '6000' and '6999'
                then '3-expense'
            end as income_statement_grouping,
            oc.transaction_lines_memo,
            oc.entity_name,
            oc.receipt_url,
            oc.status,
            oc.transaction_type,
            oc.class_id,
            oc.class_name,
            oc.department_id,
            oc.department_name,
            oc.parent_department_name,
            oc.accounting_period_id,
            oc.accounting_period,
            oc.accounting_period_name,
            dd.fiscal_year,
            dd.fiscal_quarter,
            dd.fiscal_quarter_name
        from opex_cogs oc
        left join date_details dd on dd.first_day_of_month = oc.accounting_period

    ),
    cost_category_grouping as (

        select isg.*, cc.cost_category_level_1, cc.cost_category_level_2
        from income_statement_grouping isg
        left join cost_category cc on isg.unique_account_name = cc.unique_account_name

    )

select *
from cost_category_grouping

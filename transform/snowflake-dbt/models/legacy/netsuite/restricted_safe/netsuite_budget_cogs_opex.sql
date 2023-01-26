with
    budget as (select * from {{ ref("netsuite_budget") }}),
    budget_category as (select * from {{ ref("netsuite_budget_category") }}),
    accounts as (select * from {{ ref("netsuite_accounts_xf") }}),
    accounting_periods as (select * from {{ ref("netsuite_accounting_periods") }}),
    departments as (select * from {{ ref("netsuite_departments_xf") }}),
    date_details as (

        select distinct
            first_day_of_month, fiscal_year, fiscal_quarter, fiscal_quarter_name
        from {{ ref("date_details") }}

    ),
    cost_category as (select * from {{ ref("netsuite_expense_cost_category") }}),
    budget_forecast_cogs_opex as (

        select
            a.account_id,
            a.account_number || ' - ' || a.account_name as unique_account_name,
            a.account_name,
            a.account_full_name,
            a.account_number,
            a.parent_account_number,
            a.unique_account_number,
            ap.accounting_period_id,
            ap.accounting_period_starting_date::date as accounting_period,
            ap.accounting_period_name,
            ap.accounting_period_full_name,
            d.department_id,
            d.department_name,
            coalesce(
                d.parent_department_name, 'zNeed Accounting Reclass'
            ) as parent_department_name,
            bc.budget_category,
            case
                when account_number between '5000' and '5999'
                then '2-cost of sales'
                when account_number between '6000' and '6999'
                then '3-expense'
            end as income_statement_grouping,
            sum(
                case when b.budget_amount is null then 0 else b.budget_amount end
            ) as budget_amount
        from budget b
        left join budget_category bc on b.category_id = bc.budget_category_id
        left join accounts a on b.account_id = a.account_id
        left join
            accounting_periods ap on b.accounting_period_id = ap.accounting_period_id
        left join departments d on b.department_id = d.department_id
        where
            ap.fiscal_calendar_id = 2 and a.account_number between '5000' and '6999'
            {{ dbt_utils.group_by(n=16) }}

    ),
    cost_category_grouping as (

        select
            b.*,
            dd.fiscal_year,
            dd.fiscal_quarter,
            dd.fiscal_quarter_name,
            cc.cost_category_level_1,
            cc.cost_category_level_2
        from budget_forecast_cogs_opex b
        left join date_details dd on dd.first_day_of_month = b.accounting_period
        left join cost_category cc on b.unique_account_name = cc.unique_account_name

    )

select *
from cost_category_grouping
order by accounting_period_id, account_full_name

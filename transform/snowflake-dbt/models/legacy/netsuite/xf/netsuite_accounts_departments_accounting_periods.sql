with
    accounts as (select * from {{ ref("netsuite_accounts_xf") }}),
    cost_category as (select * from {{ ref("netsuite_expense_cost_category") }}),
    date_details as (

        select distinct
            first_day_of_month, fiscal_year, fiscal_quarter, fiscal_quarter_name
        from {{ ref("date_details") }}

    ),
    departments as (select * from {{ ref("netsuite_departments_xf") }}),
    accts_depts as (

        select distinct
            accts.account_number || ' - ' || accts.account_name as unique_account_name,
            accts.account_number,
            accts.account_name,
            depts.parent_department_name,
            depts.department_name
        from accounts accts
        cross join departments depts
        where accts.is_account_inactive = false
        order by 2, 3, 4

    ),
    accts_depts_periods as (

        select distinct
            accts_depts.unique_account_name,
            accts_depts.account_number,
            accts_depts.account_name,
            accts_depts.parent_department_name,
            accts_depts.department_name,
            dd.first_day_of_month as accounting_period,
            dd.fiscal_year
        from accts_depts
        cross join date_details dd
        where
            date_trunc('year', first_day_of_month)
            between dateadd('year', -2, date_trunc('year', current_date))
            and dateadd('year', 2, date_trunc('year', current_date))

    ),
    accts_depts_periods_cost as (

        select distinct
            adp.fiscal_year,
            adp.unique_account_name,
            adp.account_number,
            adp.parent_department_name,
            adp.department_name,
            cc.cost_category_level_1,
            adp.accounting_period
        from accts_depts_periods adp
        left join cost_category cc on adp.unique_account_name = cc.unique_account_name

    )

select *
from accts_depts_periods_cost
order by
    fiscal_year desc,
    accounting_period,
    account_number,
    parent_department_name,
    department_name

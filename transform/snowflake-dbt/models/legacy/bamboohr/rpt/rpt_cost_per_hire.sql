{{ config(tags=["mnpi_exception"]) }}

with
    recruiting_expenses as (

        select
            accounting_period,
            sum(
                iff(department_name = 'Recruiting', actual_amount, 0)
            ) as recruiting_department,
            sum(iff(account_number = 6055, actual_amount, 0)) as recruiting_fees,
            sum(
                iff(
                    department_name = 'Recruiting'
                    and account_number != 6055
                    and account_number != 6075,
                    actual_amount,
                    0
                )
            ) as recruiting_department_minus_overlap,
            sum(
                iff(
                    lower(transaction_lines_memo) = 'referral bonus'
                    or account_number = 6075,
                    actual_amount,
                    0
                )
            ) as referral_fees,
            recruiting_department_minus_overlap
            + recruiting_fees
            + referral_fees as total_expenses
        from {{ ref("netsuite_actuals_income_cogs_opex") }}
        group by 1

    ),
    hires as (

        select
            date_trunc(month, hire_date) as hire_month,
            count(distinct(employee_id)) as hires
        from {{ ref("employee_directory_analysis") }}
        where is_hire_date = true
        group by 1

    ),
    joined as (

        select
            hire_month,
            hires,
            recruiting_department,
            recruiting_fees,
            recruiting_department_minus_overlap,
            referral_fees,
            total_expenses,
            total_expenses / hires as cost_per_hire,
            sum(total_expenses) over (
                order by hire_month rows between 2 preceding and current row
            ) as rolling_3_month_total_expenses,
            sum(hires) over (
                order by hire_month rows between 2 preceding and current row
            ) as rolling_3_month_hires,
            rolling_3_month_total_expenses
            / rolling_3_month_hires as rolling_3_month_cost_per_hire
        from hires
        inner join
            recruiting_expenses
            on hires.hire_month = recruiting_expenses.accounting_period

    )

select *
from joined
where hire_month > '2019-01-01'

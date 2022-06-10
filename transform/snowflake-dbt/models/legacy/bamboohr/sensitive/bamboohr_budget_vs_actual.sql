with
    dates as (select * from {{ ref("dim_date") }}),
    promotions as (select * from {{ ref("bamboohr_promotions_xf") }}),
    sheetload_people_budget as (select * from {{ ref("sheetload_people_budget") }}),
    budget as (

        select
            case
                when division = 'Engineering_Meltano'
                then 'Engineering/Meltano'
                when division = 'People_CEO'
                then 'People Group/CEO'
                when division = 'Marketing'
                then 'Marketing - Including SDR'
                else division
            end as division,
            fiscal_year,
            fiscal_quarter,
            budget,
            excess_from_previous_quarter,
            annual_comp_review
        from sheetload_people_budget

        UNION ALL

        select
            'Total - Including SDR' as division,
            fiscal_year,
            fiscal_quarter,
            sum(budget) as budget,
            sum(excess_from_previous_quarter) as excess_from_previous_quarter,
            sum(annual_comp_review) as annual_comp_review
        from sheetload_people_budget
        group by 1, 2, 3

        UNION ALL

        select
            'Total - Excluding SDR' as division,
            fiscal_year,
            fiscal_quarter,
            sum(budget) as budget,
            sum(excess_from_previous_quarter) as excess_from_previous_quarter,
            sum(annual_comp_review) as annual_comp_review
        from sheetload_people_budget
        where division != 'Sales Development'
        group by 1, 2, 3

    ),
    promotions_aggregated as (

        select
            division_grouping as division,
            dates.fiscal_year,
            dates.fiscal_quarter,
            sum(total_change_in_comp) as total_spend
        from promotions
        left join dates on promotions.promotion_month = dates.date_actual
        group by 1, 2, 3

        UNION ALL

        select
            'Marketing - Excluding SDR' as division,
            dates.fiscal_year,
            dates.fiscal_quarter,
            sum(total_change_in_comp) as total_spend
        from promotions
        left join dates on promotions.promotion_month = dates.date_actual
        where division = 'Marketing' and department != 'Sales Development'
        group by 1, 2, 3

        UNION ALL

        select
            'Sales Development' as division,
            dates.fiscal_year,
            dates.fiscal_quarter,
            sum(total_change_in_comp) as total_spend
        from promotions
        left join dates on promotions.promotion_month = dates.date_actual
        where division = 'Marketing' and department = 'Sales Development'
        group by 1, 2, 3

        UNION ALL

        select
            'Total - Including SDR' as division,
            dates.fiscal_year,
            dates.fiscal_quarter,
            sum(total_change_in_comp) as total_spend
        from promotions
        left join dates on promotions.promotion_month = dates.date_actual
        group by 1, 2, 3

        UNION ALL

        select
            'Total - Excluding SDR' as division,
            dates.fiscal_year,
            dates.fiscal_quarter,
            sum(total_change_in_comp) as total_spend
        from promotions
        left join dates on promotions.promotion_month = dates.date_actual
        where department != 'Sales Development'
        group by 1, 2, 3

    ),
    final as (

        select
            iff(
                budget.fiscal_year = 2021,
                'FY' || budget.fiscal_year || ' - Q' || budget.fiscal_quarter,
                'FY' || budget.fiscal_year
            ) as fiscal_quarter_name,
            budget.fiscal_year,
            budget.fiscal_quarter,
            budget.division,
            budget.budget,
            budget.excess_from_previous_quarter,
            coalesce(
                promotions_aggregated_fy.total_spend, promotions_aggregated.total_spend
            ) - budget.annual_comp_review as total_spend
        from budget
        left join
            promotions_aggregated
            on budget.division = promotions_aggregated.division
            and budget.fiscal_year = promotions_aggregated.fiscal_year
            and budget.fiscal_quarter = promotions_aggregated.fiscal_quarter
            and budget.fiscal_year = 2021
        left join
            promotions_aggregated as promotions_aggregated_fy
            on budget.division = promotions_aggregated_fy.division
            and budget.fiscal_year = promotions_aggregated_fy.fiscal_year
            -- Prior to FY22 the budget was determined by quarter, and since then it
            -- is based at fiscal year budget level
            and budget.fiscal_year >= 2022

    )

select *, 1 - (budget - total_spend) / budget as percent_of_budget_remaining
from final

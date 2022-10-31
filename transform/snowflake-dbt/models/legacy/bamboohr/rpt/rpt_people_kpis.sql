{{ config(tags=["mnpi_exception"]) }}

{{
    simple_cte(
        [
            ("dim_dates", "dim_date"),
            ("rpt", "bamboohr_rpt_headcount_aggregation"),
            ("rpt_headcount_vs_planned", "rpt_headcount_vs_planned"),
            (
                "rpt_team_members_out_of_comp_band",
                "rpt_team_members_out_of_comp_band",
            ),
            ("rpt_osat", "rpt_osat"),
            ("rpt_recruiting_kpis", "rpt_recruiting_kpis"),
            ("rpt_cost_per_hire", "rpt_cost_per_hire"),
            ("rpt_promotion", "bamboohr_promotion_rate"),
        ]
    )
}},
basic_metrics as (

    select
        dim_dates.fiscal_year,
        dim_dates.fiscal_quarter_name,
        dim_dates.is_first_day_of_last_month_of_fiscal_quarter,
        month_date,
        headcount_end,
        hire_count,
        sum(hire_count) over (partition by fiscal_year) as hires_fiscal_year,
        sum(hire_count) over (
            order by month_date rows between 2 preceding and current row
        ) as rolling_3_month_hires,
        sum(hire_count) over (
            partition by fiscal_year
            order by month_date
            rows between unbounded preceding and current row
        ) hires_fiscal_ytd,
        retention,
        voluntary_separation_rate,
        involuntary_separation_rate,
        location_factor,
        new_hire_location_factor_rolling_3_month,
        discretionary_bonus / headcount_end as discretionary_bonus_rate,
        rolling_12_month_promotions,
        rolling_12_month_promotions_excluding_sdr,
        rolling_12_month_promotion_increase,
        rolling_12_month_promotion_increase_excluding_sdr
    from rpt
    left join dim_dates on dim_dates.date_actual = rpt.month_date
    where breakout_type = 'kpi_breakout'

),
diversity_metrics as (

    select
        dim_dates.fiscal_year,
        dim_dates.fiscal_quarter_name,
        dim_dates.is_first_day_of_last_month_of_fiscal_quarter,
        month_date,
        sum(iff(eeoc_value = 'Female', percent_of_headcount, null)) as female_headcount,
        sum(iff(eeoc_value = 'Female', percent_of_hires, null)) as female_hires,
        sum(
            iff(eeoc_value = 'Female', percent_of_headcount_manager, null)
        ) as female_managers,
        sum(
            iff(eeoc_value = 'Female', percent_of_headcount_leaders, null)
        ) as female_leaders,
        sum(
            iff(eeoc_value = 'Female', percent_of_headcount_staff, null)
        ) as female_staff,

        sum(
            iff(
                eeoc_field_name = 'region_modified' and eeoc_value != 'NORAM',
                percent_of_headcount,
                null
            )
        ) as non_noram_headcount,

        sum(
            iff(
                eeoc_field_name = 'urg_group' and eeoc_value = true,
                percent_of_hires,
                null
            )
        ) as percent_of_urg_hires,
        sum(
            iff(
                eeoc_field_name = 'ethnicity'
                and eeoc_value = 'Black or African American',
                percent_of_headcount,
                null
            )
        ) as percent_of_headcount_black_or_african_american
    from rpt
    left join dim_dates on dim_dates.date_actual = rpt.month_date
    where breakout_type = 'eeoc_breakout'
    group by 1, 2, 3, 4

),
people_group_metrics as (

    select
        month_date,
        sum(
            iff(
                department = 'Recruiting',
                new_hire_location_factor_rolling_3_month,
                null
            )
        ) as recruiting_new_hire_location_factor,
        sum(
            iff(
                department = 'People Success',
                new_hire_location_factor_rolling_3_month,
                null
            )
        ) as people_success_new_hire_location_factor
    from rpt
    left join dim_dates on dim_dates.date_actual = rpt.month_date
    where breakout_type = 'department_breakout' and eeoc_field_name = 'no_eeoc'
    group by 1

),
greenhouse_metrics as (select * from rpt_recruiting_kpis),
final as (

    select
        basic_metrics.*,
        rpt_promotion.promotion_rate as company_promotion_rate_excluding_sdr,
        sdr_promotion.promotion_rate as sdr_promotion_rate,
        rpt_headcount_vs_planned.actual_headcount_vs_planned_headcount,
        rpt_cost_per_hire.rolling_3_month_cost_per_hire,
        rpt_team_members_out_of_comp_band.percent_of_employees_outside_of_band,

        people_group_metrics.recruiting_new_hire_location_factor,
        people_group_metrics.people_success_new_hire_location_factor,

        diversity_metrics.female_headcount,
        diversity_metrics.female_hires,
        diversity_metrics.female_managers,
        diversity_metrics.female_leaders,
        diversity_metrics.female_staff,
        diversity_metrics.non_noram_headcount,
        diversity_metrics.percent_of_headcount_black_or_african_american,
        diversity_metrics.percent_of_urg_hires,

        rpt_osat.rolling_3_month_osat,
        rpt_osat.rolling_3_month_respondents
        / basic_metrics.rolling_3_month_hires as rolling_3_month_osat_response_rate,
        rpt_osat.rolling_3_month_buddy_score,
        rpt_osat.rolling_3_month_buddy_respondents,

        rpt_recruiting_kpis.offer_acceptance_rate,
        rpt_recruiting_kpis.percent_sourced_hires,
        rpt_recruiting_kpis.percent_outbound_hires,
        rpt_recruiting_kpis.time_to_offer_median,
        rpt_recruiting_kpis.isat,
        rpt_headcount_vs_planned.cumulative_hires_vs_plan

    -- % urg
    from basic_metrics
    left join
        diversity_metrics on basic_metrics.month_date = diversity_metrics.month_date
    left join
        people_group_metrics
        on basic_metrics.month_date = people_group_metrics.month_date
    left join rpt_osat on basic_metrics.month_date = rpt_osat.completed_month
    left join
        rpt_headcount_vs_planned
        on basic_metrics.month_date
        = date_trunc(month, rpt_headcount_vs_planned.month_date)
        and rpt_headcount_vs_planned.breakout_type = 'all_company_breakout'
    left join
        rpt_recruiting_kpis on basic_metrics.month_date = rpt_recruiting_kpis.month_date
    left join
        rpt_cost_per_hire on basic_metrics.month_date = rpt_cost_per_hire.hire_month
    left join
        rpt_promotion
        on basic_metrics.month_date = rpt_promotion.month_date
        and rpt_promotion.field_name = 'company_breakout'
        and rpt_promotion.field_value = 'Company - Excluding SDR'
    left join
        rpt_promotion as sdr_promotion
        on basic_metrics.month_date = sdr_promotion.month_date
        and sdr_promotion.field_name = 'department_grouping_breakout'
        and sdr_promotion.field_value = 'Sales Development'
    left join
        rpt_team_members_out_of_comp_band
        on basic_metrics.month_date
        = date_trunc(month, rpt_team_members_out_of_comp_band.date_actual)
        and rpt_team_members_out_of_comp_band.breakout_type = 'company_breakout'
)

select
    iff(
        month_date = dateadd(month, -1, date_trunc(month, current_date())), true, false
    ) as current_reporting_month,
    iff(
        month_date = dateadd(month, -2, date_trunc(month, current_date())), true, false
    ) as previous_reporting_month,
    iff(
        month_date = dateadd(month, -13, date_trunc(month, current_date())), true, false
    ) as last_year_reporting_month,
    dense_rank() over (order by fiscal_quarter_name desc) as rank_fiscal_quarter_desc,
    final.*
from final
where
    month_date
    between dateadd(month, -13, date_trunc(month, current_date())) and dateadd(
        month, -1, date_trunc(month, current_date())
    )

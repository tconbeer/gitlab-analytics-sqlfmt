with
    promotions as (select * from {{ ref("bamboohr_promotions_xf") }}),
    bamboohr_base as (

        select
            dateadd(month, -11, date_actual) as rolling_start_month,
            date_actual as rolling_end_month,
            field_name,
            field_value
        from {{ ref("bamboohr_base_mapping") }}

    ),
    headcount_end as (

        select
            month_date,
            case
                when breakout_type = 'kpi_breakout'
                then 'company_breakout'
                when breakout_type = 'department_breakout'
                then 'department_grouping_breakout'
                when breakout_type = 'division_breakout'
                then 'division_grouping_breakout'
                else null
            end as breakout_type,
            case
                when breakout_type = 'kpi_breakout'
                then 'company_breakout'
                when breakout_type = 'division_breakout'
                then {{ bamboohr_division_grouping(division="division") }}
                else {{ bamboohr_department_grouping(department="department") }}
            end as division_department,
            sum(headcount_end) as headcount_end,
            sum(headcount_end_excluding_sdr) as headcount_end_excluding_sdr
        from {{ ref("bamboohr_rpt_headcount_aggregation") }}
        where
            breakout_type in (
                'department_breakout', 'kpi_breakout', 'division_breakout'
            )
            and eeoc_field_name = 'no_eeoc'
        group by 1, 2, 3


    ),
    joined as (

        select bamboohr_base.*, promotions.*, headcount_end
        from bamboohr_base
        left join
            promotions
            on promotions.promotion_month
            between rolling_start_month and rolling_end_month
            and iff(
                field_name = 'division_grouping_breakout',
                promotions.division_grouping,
                promotions.department_grouping
            )
            = bamboohr_base.field_value
        left join
            headcount_end
            on bamboohr_base.rolling_end_month = headcount_end.month_date
            and bamboohr_base.field_name = headcount_end.breakout_type
            and bamboohr_base.field_value = headcount_end.division_department
        where bamboohr_base.field_name != 'company_breakout'

        union all

        select
            bamboohr_base.rolling_start_month,
            bamboohr_base.rolling_end_month,
            'division_grouping_breakout' as field_name,
            'Marketing - Excluding SDR' as field_value,
            promotions.*,
            headcount_end_excluding_sdr
        from bamboohr_base
        inner join
            promotions
            on promotions.promotion_month
            between rolling_start_month and rolling_end_month
            and iff(
                field_name = 'division_grouping_breakout',
                promotions.division_grouping,
                promotions.department_grouping
            )
            = bamboohr_base.field_value
        left join
            headcount_end
            on bamboohr_base.rolling_end_month = headcount_end.month_date
            and bamboohr_base.field_name = headcount_end.breakout_type
            and bamboohr_base.field_value = headcount_end.division_department
        where
            bamboohr_base.field_name = 'division_grouping_breakout'
            and promotions.division_grouping = 'Marketing'
            and promotions.department != 'Sales Development'

        union all

        select bamboohr_base.*, promotions.*, headcount_end
        from bamboohr_base
        left join
            promotions
            on promotions.promotion_month
            between rolling_start_month and rolling_end_month
        left join
            headcount_end
            on bamboohr_base.rolling_end_month = headcount_end.month_date
            and bamboohr_base.field_name = headcount_end.breakout_type
            and bamboohr_base.field_value = headcount_end.division_department
        where bamboohr_base.field_name = 'company_breakout'

        union all

        select
            bamboohr_base.rolling_start_month,
            bamboohr_base.rolling_end_month,
            'company_breakout' as field_name,
            'Company - Excluding SDR' as field_value,
            promotions.*,
            headcount_end_excluding_sdr
        from bamboohr_base
        left join
            promotions
            on promotions.promotion_month
            between rolling_start_month and rolling_end_month
        left join
            headcount_end
            on bamboohr_base.rolling_end_month = headcount_end.month_date
            and bamboohr_base.field_name = headcount_end.breakout_type
            and bamboohr_base.field_value = headcount_end.division_department
        where
            bamboohr_base.field_name = 'company_breakout'
            and promotions.department != 'Sales Development'

    ),
    final as (

        select
            rolling_end_month as month_date,
            field_name,
            field_value,
            headcount_end,
            count(employee_id) as total_promotions,
            total_promotions / nullifzero(headcount_end) as promotion_rate,
            iff(
                total_promotions <= 3, null, avg(percent_change_in_comp)
            ) as average_percent_change_in_comp,
            iff(
                total_promotions <= 3, null, median(percent_change_in_comp)
            ) as median_percent_change_change_in_comp
        from joined
        group by 1, 2, 3, 4

    )

select *
from final

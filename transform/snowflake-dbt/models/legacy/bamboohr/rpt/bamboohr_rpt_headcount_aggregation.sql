{% set partition_statement = "OVER (PARTITION BY base.breakout_type, base.department, base.division, base.job_role,                                     base.job_grade, base.eeoc_field_name, base.eeoc_value                               ORDER BY base.month_date DESC ROWS BETWEEN CURRENT ROW AND 11 FOLLOWING)                               " %}
{% set ratio_to_report_partition_statement = "OVER (PARTITION BY base.month_date, base.breakout_type, base.department, base.division, base.job_role,                                               base.job_grade, base.eeoc_field_name                                               ORDER BY base.month_date)                               " %}

with
    source as (select * from {{ ref("bamboohr_headcount_intermediate") }}),
    base as (

        select distinct
            unique_key,
            month_date,
            breakout_type_modified as breakout_type,
            department,
            division,
            job_role,
            job_grade,
            eeoc_field_name,
            eeoc_value
        -- - this is to group groups with less than 4 headcount
        from source

    ),
    intermediate as (

        select
            base.month_date,
            iff(
                base.month_date = dateadd(month, -1, date_trunc(month, current_date())),
                true,
                false
            ) as is_last_month,
            base.breakout_type,
            base.department,
            base.division,
            base.job_role,
            base.job_grade,
            base.eeoc_field_name,
            base.eeoc_value,
            iff(
                base.breakout_type != 'eeoc_breakout'
                and base.eeoc_field_name != 'no_eeoc',
                false,
                true
            ) as show_value_criteria,
            headcount_start,
            headcount_end,
            headcount_end_excluding_sdr,
            headcount_average,
            hire_count,
            separation_count,
            voluntary_separation,
            involuntary_separation,
            avg(
                coalesce(headcount_average, 0)
            ) {{ partition_statement }} as rolling_12_month_headcount,
            sum(
                coalesce(separation_count, 0)
            ) {{ partition_statement }} as rolling_12_month_separations,
            sum(
                coalesce(voluntary_separation, 0)
            ) {{ partition_statement }} as rolling_12_month_voluntary_separations,
            sum(
                coalesce(involuntary_separation, 0)
            ) {{ partition_statement }} as rolling_12_month_involuntary_separations,
            iff(
                rolling_12_month_headcount < rolling_12_month_separations,
                null,
                1 - (
                    rolling_12_month_separations / nullif(rolling_12_month_headcount, 0)
                )
            ) as retention,

            headcount_end_leader,
            headcount_average_leader,
            hired_leaders,
            separated_leaders,
            avg(
                coalesce(headcount_average_leader, 0)
            ) {{ partition_statement }} as rolling_12_month_headcount_leader,
            sum(
                coalesce(separated_leaders, 0)
            ) {{ partition_statement }} as rolling_12_month_separations_leader,
            iff(
                rolling_12_month_headcount_leader < rolling_12_month_separations_leader,
                null,
                1 - (
                    rolling_12_month_separations_leader / nullif(
                        rolling_12_month_headcount_leader, 0
                    )
                )
            ) as retention_leader,

            headcount_end_manager,
            headcount_average_manager,
            hired_manager,
            separated_manager,
            avg(
                coalesce(headcount_average_manager, 0)
            ) {{ partition_statement }} as rolling_12_month_headcount_manager,
            sum(
                coalesce(separated_manager, 0)
            ) {{ partition_statement }} as rolling_12_month_separations_manager,
            iff(
                rolling_12_month_headcount_manager
                < rolling_12_month_separations_manager,
                null,
                1 - (
                    rolling_12_month_separations_manager / nullif(
                        rolling_12_month_headcount_manager, 0
                    )
                )
            ) as retention_manager,

            headcount_end_management,
            headcount_average_management,
            hired_management,
            separated_management,
            avg(
                coalesce(headcount_average_management, 0)
            ) {{ partition_statement }} as rolling_12_month_headcount_management,
            sum(
                coalesce(separated_management, 0)
            ) {{ partition_statement }} as rolling_12_month_separations_management,
            iff(
                rolling_12_month_headcount_management
                < rolling_12_month_separations_management,
                null,
                1 - (
                    rolling_12_month_separations_management /
                    nullif(rolling_12_month_headcount_management, 0
                    )
                )
            ) as retention_management,


            headcount_end_staff,
            headcount_average_staff,
            hired_staff,
            separated_staff,
            avg(
                coalesce(headcount_average_staff, 0)
            ) {{ partition_statement }} as rolling_12_month_headcount_staff,
            sum(
                coalesce(separated_staff, 0)
            ) {{ partition_statement }} as rolling_12_month_separations_staff,
            iff(
                rolling_12_month_headcount_staff < rolling_12_month_separations_staff,
                null,
                1 - (
                    rolling_12_month_separations_management /
                    nullif(rolling_12_month_headcount_staff, 0
                    )
                )
            ) as retention_staff,


            headcount_end_individual_contributor,
            headcount_average_contributor,
            hired_contributor,
            separated_contributor,

            min(headcount_end_individual_contributor)
            {{ ratio_to_report_partition_statement }} as min_headcount_end_contributor,
            sum(headcount_end_individual_contributor)
            {{ ratio_to_report_partition_statement }}
            as total_headcount_end_contributor,
            min(
                headcount_average
            ) {{ ratio_to_report_partition_statement }} as min_headcount_average,
            sum(
                headcount_end
            ) {{ ratio_to_report_partition_statement }} as total_headcount_end,
            min(hire_count) {{ ratio_to_report_partition_statement }} as min_hire_count,
            sum(
                hire_count
            ) {{ ratio_to_report_partition_statement }} as total_hire_count,
            min(
                headcount_average_leader
            ) {{ ratio_to_report_partition_statement }} as min_headcount_leader,
            sum(
                headcount_average_leader
            ) {{ ratio_to_report_partition_statement }} as total_headcount_leader,
            min(
                headcount_average_manager
            ) {{ ratio_to_report_partition_statement }} as min_headcount_manager,
            sum(
                headcount_average_manager
            ) {{ ratio_to_report_partition_statement }} as total_headcount_manager,
            min(
                headcount_average_staff
            ) {{ ratio_to_report_partition_statement }} as min_headcount_staff,
            sum(
                headcount_average_staff
            ) {{ ratio_to_report_partition_statement }} as total_headcount_staff,
            min(
                headcount_average_contributor
            ) {{ ratio_to_report_partition_statement }} as min_headcount_contributor,


            ratio_to_report(headcount_end)
            {{ ratio_to_report_partition_statement }} as percent_of_headcount,
            ratio_to_report(hire_count)
            {{ ratio_to_report_partition_statement }} as percent_of_hires,
            ratio_to_report(headcount_end_leader)
            {{ ratio_to_report_partition_statement }} as percent_of_headcount_leaders,
            ratio_to_report(headcount_end_manager)
            {{ ratio_to_report_partition_statement }} as percent_of_headcount_manager,
            ratio_to_report(headcount_end_staff)
            {{ ratio_to_report_partition_statement }} as percent_of_headcount_staff,
            ratio_to_report(headcount_end_individual_contributor)
            {{ ratio_to_report_partition_statement }}
            as percent_of_headcount_contributor,

            sum(
                coalesce(promotion, 0)
            ) {{ partition_statement }} as rolling_12_month_promotions,
            sum(
                coalesce(promotion_excluding_sdr, 0)
            ) {{ partition_statement }} as rolling_12_month_promotions_excluding_sdr,

            sum(coalesce(percent_change_in_comp, 0)) {{ partition_statement }}
            as rolling_12_month_promotions_percent_change_in_comp,
            sum(
                coalesce(percent_change_in_comp_excluding_sdr, 0)
            ) {{ partition_statement }}
            as rolling_12_month_promotions_percent_change_in_comp_excluding_sdr,
            location_factor,
            avg(new_hire_location_factor) over (
                partition by
                    base.breakout_type,
                    base.department,
                    base.division,
                    base.job_role,
                    base.job_grade,
                    base.eeoc_field_name,
                    base.eeoc_value
                order by base.month_date desc
                rows between current row and 2 following
            ) as new_hire_location_factor_rolling_3_month,
            discretionary_bonus,
            tenure_months,
            tenure_zero_to_six_months,
            tenure_six_to_twelve_months,
            tenure_one_to_two_years,
            tenure_two_to_four_years,
            tenure_four_plus_years
        from base
        left join source on base.unique_key = source.unique_key
        where base.month_date < date_trunc('month', current_date)

    ),
    final as (

        select
            month_date,
            is_last_month,
            breakout_type,
            department,
            division,
            job_role,
            job_grade,
            eeoc_field_name,
            case
                when eeoc_field_name = 'gender' and headcount_end < 5
                then 'Other'
                when eeoc_field_name = 'gender-region' and headcount_end < 5
                then 'Other_' || split_part(eeoc_value, '_', 2)
                when eeoc_field_name = 'ethnicity' and headcount_end < 5
                then 'Other'
                else eeoc_value
            end as eeoc_value,
            iff(
                headcount_start < 4 and show_value_criteria = false,
                null,
                headcount_start
            ) as headcount_start,
            iff(
                headcount_end < 4 and show_value_criteria = false, null, headcount_end
            ) as headcount_end,
            iff(
                headcount_end_excluding_sdr < 4 and show_value_criteria = false,
                null,
                headcount_end_excluding_sdr
            ) as headcount_end_excluding_sdr,
            iff(
                headcount_average < 4 and eeoc_field_name != 'no_eeoc',
                null,
                headcount_average
            ) as headcount_average,
            iff(
                hire_count < 4 and eeoc_field_name != 'no_eeoc', null, hire_count
            ) as hire_count,
            iff(
                separation_count < 4 and eeoc_field_name != 'no_eeoc',
                null,
                separation_count
            ) as separation_count,
            iff(
                voluntary_separation < 4, null, voluntary_separation
            ) as voluntary_separation_count,
            iff(
                voluntary_separation < 4, null, involuntary_separation
            ) as involuntary_separation_count,

            rolling_12_month_headcount,
            rolling_12_month_separations,
            rolling_12_month_voluntary_separations,
            rolling_12_month_involuntary_separations,
            iff(
                rolling_12_month_headcount < rolling_12_month_voluntary_separations,
                null,
                (
                    rolling_12_month_voluntary_separations / nullif(
                        rolling_12_month_headcount, 0
                    )
                )
            ) as voluntary_separation_rate,
            iff(
                rolling_12_month_headcount < rolling_12_month_involuntary_separations,
                null,
                (
                    rolling_12_month_involuntary_separations / nullif(
                        rolling_12_month_headcount, 0
                    )
                )
            ) as involuntary_separation_rate,
            retention,

            iff(
                headcount_end_leader < 2 and eeoc_field_name != 'no_eeoc',
                null,
                headcount_end_leader
            ) as headcount_end_leader,
            iff(
                headcount_average_leader < 2 and eeoc_field_name != 'no_eeoc',
                null,
                headcount_average_leader
            ) as headcount_leader_average,
            iff(
                hired_leaders < 2 and eeoc_field_name != 'no_eeoc', null, hired_leaders
            ) as hired_leaders,
            iff(
                separated_leaders < 2 and eeoc_field_name != 'no_eeoc',
                null,
                separated_leaders
            ) as separated_leaders,
            rolling_12_month_headcount_leader,
            rolling_12_month_separations_leader,
            retention_leader,


            iff(
                headcount_end_manager < 2 and eeoc_field_name != 'no_eeoc',
                null,
                headcount_end_manager
            ) as headcount_end_manager,
            iff(
                headcount_average_manager < 2 and eeoc_field_name != 'no_eeoc',
                null,
                headcount_average_manager
            ) as headcount_manager_average,
            iff(
                hired_manager < 2 and eeoc_field_name != 'no_eeoc', null, hired_manager
            ) as hired_manager,
            iff(
                separated_manager < 2 and eeoc_field_name != 'no_eeoc',
                null,
                separated_manager
            ) as separated_manager,
            rolling_12_month_headcount_manager,
            rolling_12_month_separations_manager,
            retention_manager,

            iff(
                headcount_end_management < 2 and eeoc_field_name != 'no_eeoc',
                null,
                headcount_end_management
            ) as headcount_end_management,
            iff(
                headcount_average_management < 2 and eeoc_field_name != 'no_eeoc',
                null,
                headcount_average_management
            ) as headcount_management_average,
            iff(
                hired_management < 2 and eeoc_field_name != 'no_eeoc',
                null,
                hired_management
            ) as hired_management,
            iff(
                separated_management < 2 and eeoc_field_name != 'no_eeoc',
                null,
                separated_management
            ) as separated_management,
            rolling_12_month_headcount_management,
            rolling_12_month_separations_management,
            retention_management,

            iff(
                headcount_end_staff < 3 and eeoc_field_name != 'no_eeoc',
                null,
                headcount_end_staff
            ) as headcount_end_staff,
            iff(
                headcount_average_staff < 3 and eeoc_field_name != 'no_eeoc',
                null,
                headcount_average_staff
            ) as headcount_average_staff,
            iff(
                hired_staff < 3 and eeoc_field_name != 'no_eeoc', null, hired_staff
            ) as hired_staff,
            iff(
                separated_staff < 3 and eeoc_field_name != 'no_eeoc',
                null,
                separated_staff
            ) as separated_staff,

            iff(
                headcount_end_individual_contributor < 4
                and eeoc_field_name != 'no_eeoc',
                null,
                headcount_end_individual_contributor
            ) as headcount_end_contributor,
            iff(
                headcount_average_contributor < 4 and eeoc_field_name != 'no_eeoc',
                null,
                headcount_average_contributor
            ) as headcount_contributor,
            iff(
                hired_contributor < 4 and eeoc_field_name != 'no_eeoc',
                null,
                hired_contributor
            ) as hired_contributor,
            iff(
                separated_contributor < 4 and eeoc_field_name != 'no_eeoc',
                null,
                separated_contributor
            ) as separated_contributor,

            iff(
                total_headcount_end < 5 and show_value_criteria = false,
                null,
                percent_of_headcount
            ) as percent_of_headcount,
            iff(
                total_hire_count < 5 and show_value_criteria = false,
                null,
                percent_of_hires
            ) as percent_of_hires,
            iff(
                total_headcount_leader < 3 and show_value_criteria = false,
                null,
                percent_of_headcount_leaders
            ) as percent_of_headcount_leaders,
            iff(
                total_headcount_manager < 3 and show_value_criteria = false,
                null,
                percent_of_headcount_manager
            ) as percent_of_headcount_manager,
            iff(
                (total_headcount_staff < 5 and show_value_criteria = false)
                or (
                    breakout_type = 'all_attributes_breakout'
                    and eeoc_field_name != 'no_eeoc'
                ),
                null,
                percent_of_headcount_staff
            ) as percent_of_headcount_staff,
            iff(
                total_headcount_end_contributor < 5 and show_value_criteria = false,
                null,
                percent_of_headcount_contributor
            ) as percent_of_headcount_contributor,

            case
                when
                    breakout_type in (
                        'kpi_breakout', 'division_breakout', 'department_breakout'
                    )
                    and eeoc_value = 'no_eeoc'
                then rolling_12_month_promotions
                when
                    breakout_type in ('eeoc_breakout')
                    and eeoc_field_name in ('gender', 'ethnicity', 'region_modified')
                    and rolling_12_month_promotions > 3
                then rolling_12_month_promotions
                else null
            end as rolling_12_month_promotions,

            case
                when
                    breakout_type in (
                        'kpi_breakout', 'division_breakout', 'department_breakout'
                    )
                    and eeoc_value = 'no_eeoc'
                then rolling_12_month_promotions_excluding_sdr
                when
                    breakout_type in ('eeoc_breakout')
                    and eeoc_field_name in ('gender', 'ethnicity', 'region_modified')
                    and rolling_12_month_promotions > 3
                then rolling_12_month_promotions_excluding_sdr
                else null
            end as rolling_12_month_promotions_excluding_sdr,
            case
                when
                    breakout_type in (
                        'kpi_breakout', 'division_breakout', 'department_breakout'
                    )
                    and eeoc_value = 'no_eeoc'
                    and rolling_12_month_promotions > 3
                then
                    rolling_12_month_promotions_percent_change_in_comp
                    / rolling_12_month_promotions
                when
                    breakout_type in ('eeoc_breakout')
                    and eeoc_field_name in ('gender', 'ethnicity', 'region_modified')
                    and rolling_12_month_promotions > 3
                then
                    rolling_12_month_promotions_percent_change_in_comp
                    / rolling_12_month_promotions
                else null
            end as rolling_12_month_promotion_increase,
            case
                when
                    breakout_type in (
                        'kpi_breakout', 'division_breakout', 'department_breakout'
                    )
                    and eeoc_value = 'no_eeoc'
                    and rolling_12_month_promotions_excluding_sdr > 3
                then
                    rolling_12_month_promotions_percent_change_in_comp_excluding_sdr
                    / rolling_12_month_promotions_excluding_sdr
                when
                    breakout_type in ('eeoc_breakout')
                    and eeoc_field_name in ('gender', 'ethnicity', 'region_modified')
                    and rolling_12_month_promotions_excluding_sdr > 3
                then
                    rolling_12_month_promotions_percent_change_in_comp_excluding_sdr
                    / rolling_12_month_promotions_excluding_sdr
                else null
            end as rolling_12_month_promotion_increase_excluding_sdr,

            iff(
                headcount_end < 4 and show_value_criteria = false, null, location_factor
            ) as location_factor,

            new_hire_location_factor_rolling_3_month,
            iff(
                discretionary_bonus < 4 and show_value_criteria = false,
                null,
                discretionary_bonus
            ) as discretionary_bonus,
            iff(
                tenure_months < 4 and show_value_criteria = false, null, tenure_months
            ) as tenure_months,
            iff(
                tenure_zero_to_six_months < 4 and show_value_criteria = false,
                null,
                tenure_zero_to_six_months
            ) as tenure_zero_to_six_months,
            iff(
                tenure_six_to_twelve_months < 4 and show_value_criteria = false,
                null,
                tenure_six_to_twelve_months
            ) as tenure_six_to_twelve_months,
            iff(
                tenure_one_to_two_years < 4 and show_value_criteria = false,
                null,
                tenure_one_to_two_years
            ) as tenure_one_to_two_years,
            iff(
                tenure_two_to_four_years < 4 and show_value_criteria = false,
                null,
                tenure_two_to_four_years
            ) as tenure_two_to_four_years,
            iff(
                tenure_four_plus_years < 4 and show_value_criteria = false,
                null,
                tenure_four_plus_years
            ) as tenure_four_plus_years
        from intermediate

    )

select *
from final

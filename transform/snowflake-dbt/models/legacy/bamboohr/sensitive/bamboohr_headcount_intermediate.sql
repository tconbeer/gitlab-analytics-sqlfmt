{{
    config(
        {
            "materialized": "table",
        }
    )
}}

{% set repeated_metric_columns = "SUM(headcount_start)                             AS headcount_start,       SUM(headcount_end)                                AS headcount_end,       SUM(headcount_end_excluding_sdr)                  AS headcount_end_excluding_sdr,       (SUM(headcount_start) + SUM(headcount_end))/2     AS headcount_average,       SUM(hire_count)                                   AS hire_count,       SUM(separation_count)                             AS separation_count,       SUM(voluntary_separation)                         AS voluntary_separation,       SUM(involuntary_separation)                       AS involuntary_separation,        SUM(headcount_start_leader)                       AS headcount_start_leader,       SUM(headcount_end_leader)                         AS headcount_end_leader,       (SUM(headcount_start_leader)          + SUM(headcount_end_leader))/2                  AS headcount_average_leader,       SUM(hired_leaders)                                AS hired_leaders,       SUM(separated_leaders)                            AS separated_leaders,        SUM(headcount_start_manager)                      AS headcount_start_manager,       SUM(headcount_end_manager)                        AS headcount_end_manager,       (SUM(headcount_start_manager)          + SUM(headcount_end_leader))/2                  AS headcount_average_manager,       SUM(hired_manager)                                AS hired_manager,       SUM(separated_manager)                            AS separated_manager,        SUM(headcount_start_management)                   AS headcount_start_management,       SUM(headcount_end_management)                     AS headcount_end_management,       (SUM(headcount_start_management)          + SUM(headcount_end_management))/2              AS headcount_average_management,       SUM(hired_management)                             AS hired_management,       SUM(separated_management)                         AS separated_management,                     SUM(headcount_start_staff)                        AS headcount_start_staff,       SUM(headcount_end_staff)                          AS headcount_end_staff,       (SUM(headcount_start_staff)          + SUM(headcount_end_staff))/2                   AS headcount_average_staff,       SUM(hired_staff)                                  AS hired_staff,       SUM(separated_staff)                              AS separated_staff,        SUM(headcount_start_contributor)                  AS headcount_start_contributor,       SUM(headcount_end_contributor)                    AS headcount_end_individual_contributor,       (SUM(headcount_start_contributor)          + SUM(headcount_end_contributor))/2             AS headcount_average_contributor,       SUM(hired_contributor)                            AS hired_contributor,       SUM(separated_contributor)                        AS separated_contributor,        SUM(IFF(is_promotion = TRUE,1,0))                 AS promotion,       SUM(IFF(is_promotion_excluding_sdr = TRUE,1,0))   AS promotion_excluding_sdr,              SUM(percent_change_in_comp)                       AS percent_change_in_comp,       SUM(percent_change_in_comp_excluding_sdr)         AS percent_change_in_comp_excluding_sdr,        AVG(location_factor)                              AS location_factor,       AVG(new_hire_location_factor)                     AS new_hire_location_factor,       SUM(discretionary_bonus)                          AS discretionary_bonus,        AVG(tenure_months)                                AS tenure_months,       SUM(tenure_zero_to_six_months)                    AS tenure_zero_to_six_months,       SUM(tenure_six_to_twelve_months)                  AS tenure_six_to_twelve_months,       SUM(tenure_one_to_two_years)                      AS tenure_one_to_two_years,       SUM(tenure_two_to_four_years)                     AS tenure_two_to_four_years,       SUM(tenure_four_plus_years)                       AS tenure_four_plus_years       " %}



with
    dates as (

        select date_actual as start_date, last_day(date_actual) as end_date
        from {{ ref("date_details") }}
        where
            date_day <= last_day(
                current_date
            -- min employment_status_date in bamboohr_employment_status model
            ) and day_of_month = 1 and date_actual >= '2013-07-01'

    ),
    mapping as (

        {{
            dbt_utils.unpivot(
                relation=ref("bamboohr_id_employee_number_mapping"),
                cast_to="varchar",
                exclude=[
                    "employee_number",
                    "employee_id",
                    "first_name",
                    "last_name",
                    "hire_date",
                    "termination_date",
                    "greenhouse_candidate_id",
                    "region",
                    "country",
                    "nationality",
                    "last_updated_date",
                ],
            )
        }}

    ),
    mapping_enhanced as (

        select
            employee_id,
            lower(field_name) as eeoc_field_name,
            coalesce(value, 'Not Identified') as eeoc_value
        from mapping

        union all

        select 
      distinct employee_id, 'no_eeoc' as eeoc_field_name, 'no_eeoc' as eeoc_value
        from mapping

    ),
    separation_reason as (

        select *
        from {{ ref("bamboohr_employment_status_xf") }}
        where employment_status = 'Terminated'

    ),
    employees as (select * from {{ ref("employee_directory_intermediate") }}),
    bamboohr_promotion as (select * from {{ ref("bamboohr_promotions_xf") }}),
    intermediate as (

        select
            employees.date_actual,
            employees.department_modified as department,
            division_mapped_current as division,
            -- using the current division - department mapping for reporting
            job_role_modified as job_role,
            coalesce(job_grade, 'NA') as job_grade,
            mapping_enhanced.eeoc_field_name,
            mapping_enhanced.eeoc_value,
            iff(dates.start_date = date_actual, 1, 0) as headcount_start,
            iff(dates.end_date = date_actual, 1, 0) as headcount_end,
            iff(
                dates.end_date = date_actual
                and employees.department_modified != 'Sales Development',
                1,
                0
            ) as headcount_end_excluding_sdr,
            iff(is_hire_date = true, 1, 0) as hire_count,
            iff(
                termination_type = 'Resignation (Voluntary)', 1, 0
            ) as voluntary_separation,
            iff(
                termination_type = 'Termination (Involuntary)', 1, 0
            ) as involuntary_separation,
            voluntary_separation + involuntary_separation as separation_count,

            iff(
                dates.start_date = date_actual
                and job_role_modified = 'Senior Leadership',
                1,
                0
            ) as headcount_start_leader,
            iff(
                dates.end_date = date_actual
                and job_role_modified = 'Senior Leadership',
                1,
                0
            ) as headcount_end_leader,
            iff(
                is_hire_date = true and job_role_modified = 'Senior Leadership', 1, 0
            ) as hired_leaders,
            iff(
                is_termination_date = true and job_role_modified = 'Senior Leadership',
                1,
                0
            ) as separated_leaders,

            iff(
                dates.start_date = date_actual and job_role_modified = 'Manager', 1, 0
            ) as headcount_start_manager,
            iff(
                dates.end_date = date_actual and job_role_modified = 'Manager', 1, 0
            ) as headcount_end_manager,
            iff(
                is_hire_date = true and job_role_modified = 'Manager', 1, 0
            ) as hired_manager,
            iff(
                is_termination_date = true and job_role_modified = 'Manager', 1, 0
            ) as separated_manager,

            iff(
                dates.start_date = date_actual
                and job_role_modified != 'Individual Contributor',
                1,
                0
            ) as headcount_start_management,
            iff(
                dates.end_date = date_actual
                and job_role_modified != 'Individual Contributor',
                1,
                0
            ) as headcount_end_management,
            iff(
                is_hire_date = true and job_role_modified != 'Individual Contributor',
                1,
                0
            ) as hired_management,
            iff(
                is_termination_date = true
                and job_role_modified != 'Individual Contributor',
                1,
                0
            ) as separated_management,

            iff(
                dates.start_date = date_actual and job_role_modified = 'Staff', 1, 0
            ) as headcount_start_staff,
            iff(
                dates.end_date = date_actual and job_role_modified = 'Staff', 1, 0
            ) as headcount_end_staff,
            iff(
                is_hire_date = true and job_role_modified = 'Staff', 1, 0
            ) as hired_staff,
            iff(
                is_termination_date = true and job_role_modified = 'Staff', 1, 0
            ) as separated_staff,

            iff(
                dates.start_date = date_actual
                and job_role_modified = 'Individual Contributor',
                1,
                0
            ) as headcount_start_contributor,
            iff(
                dates.end_date = date_actual
                and job_role_modified = 'Individual Contributor',
                1,
                0
            ) as headcount_end_contributor,
            iff(
                is_hire_date = true and job_role_modified = 'Individual Contributor',
                1,
                0
            ) as hired_contributor,
            iff(
                is_termination_date = true
                and job_role_modified = 'Individual Contributor',
                1,
                0
            ) as separated_contributor,


            iff(
                employees.job_title like '%VP%', 'Exclude', is_promotion
            ) as is_promotion,
            iff(
                employees.job_title like '%VP%'
                or employees.department_modified = 'Sales Development',
                'Exclude',
                is_promotion
            ) as is_promotion_excluding_sdr,
            iff(
                is_promotion = true and employees.job_title not like '%VP%',
                percent_change_in_comp,
                null
            ) as percent_change_in_comp,
            iff(
                employees.job_title like '%VP%'
                or employees.department_modified = 'Sales Development',
                null,
                percent_change_in_comp
            ) as percent_change_in_comp_excluding_sdr,

            iff(
                dates.end_date = date_actual and coalesce(
                    sales_geo_differential, 'n/a - Comp Calc'
                ) = 'n/a - Comp Calc',
                location_factor,
                null
            ) as location_factor,
            iff(
                is_hire_date = true and coalesce(
                    sales_geo_differential, 'n/a - Comp Calc'
                ) = 'n/a - Comp Calc',
                location_factor,
                null
            ) as new_hire_location_factor,
            discretionary_bonus,
            round( (tenure_days / 30), 2) as tenure_months,
            iff(
                tenure_months between 0 and 6 and dates.end_date = date_actual, 1, 0
            ) as tenure_zero_to_six_months,
            iff(
                tenure_months between 6 and 12 and dates.end_date = date_actual, 1, 0
            ) as tenure_six_to_twelve_months,
            iff(
                tenure_months between 12 and 24 and dates.end_date = date_actual, 1, 0
            ) as tenure_one_to_two_years,
            iff(
                tenure_months between 24 and 48 and dates.end_date = date_actual, 1, 0
            ) as tenure_two_to_four_years,
            iff(
                tenure_months >= 48 and dates.end_date = date_actual, 1, 0
            ) as tenure_four_plus_years
        from dates
        left join
            employees on date_trunc(month, dates.start_date) = date_trunc(
                month, employees.date_actual
            )
        left join
            mapping_enhanced on employees.employee_id = mapping_enhanced.employee_id
        left join
            separation_reason
            on separation_reason.employee_id = employees.employee_id
            and employees.date_actual = separation_reason.valid_from_date
        left join
            bamboohr_promotion
            on employees.employee_id = bamboohr_promotion.employee_id
            and employees.date_actual = bamboohr_promotion.promotion_date
        where date_actual is not null


    ),
    aggregated as (

        select
            date_trunc(month, start_date) as month_date,
            'all_attributes_breakout' as breakout_type,
            department,
            division,
            job_role,
            job_grade,
            eeoc_field_name,
            eeoc_value,
            {{ repeated_metric_columns }}
        from dates
        left join
            intermediate on date_trunc(month, start_date) = date_trunc(
                month, date_actual
            )
            {{ dbt_utils.group_by(n=8) }}


        union all

        select
            date_trunc(month, start_date) as month_date,
            'department_breakout' as breakout_type,
            department,
            division,
            null as job_role,
            null as job_grade,
            eeoc_field_name,
            eeoc_value,
            {{ repeated_metric_columns }}
        from dates
        left join
            intermediate on date_trunc(month, start_date) = date_trunc(
                month, date_actual
            )
            {{ dbt_utils.group_by(n=8) }}

        union all

        select
            date_trunc(month, start_date) as month_date,
            'eeoc_breakout' as breakout_type,
            'eeoc_breakout' as department,
            'eeoc_breakout' as division,
            null as job_role,
            null as job_grade,
            eeoc_field_name,
            eeoc_value,
            {{ repeated_metric_columns }}
        from dates
        left join
            intermediate on date_trunc(month, start_date) = date_trunc(
                month, date_actual
            )
            {{ dbt_utils.group_by(n=8) }}

        union all

        select
            date_trunc(month, start_date) as month_date,
            'division_breakout' as breakout_type,
            'division_breakout' as department,
            division,
            null as job_role,
            null as job_grade,
            eeoc_field_name,
            eeoc_value,
            {{ repeated_metric_columns }}
        from dates
        left join
            intermediate on date_trunc(month, start_date) = date_trunc(
                month, date_actual
            )
        where department is not null {{ dbt_utils.group_by(n=8) }}

    ),
    breakout_modified as (

        select
            aggregated.*,
            iff(
                breakout_type = 'eeoc_breakout' and eeoc_field_name = 'no_eeoc',
                'kpi_breakout',
                breakout_type
            ) as breakout_type_modified
        from aggregated

    ),
    final as (

        select
            {{
                dbt_utils.surrogate_key(
                    [
                        "month_date",
                        "breakout_type_modified",
                        "department",
                        "division",
                        "job_role",
                        "job_grade",
                        "eeoc_field_name",
                        "eeoc_value",
                    ]
                )
            }} as unique_key, breakout_modified.*
        from breakout_modified

    )

select *
from final

{{
    config(
        {
            "materialized": "table",
        }
    )
}}

with recursive
    employee_directory as (

        select
            employee_id,
            employee_number,
            first_name,
            last_name,
            (first_name || ' ' || last_name) as full_name,
            hire_date,
            rehire_date,
            termination_date,
            hire_location_factor,
            last_work_email
        from {{ ref("employee_directory") }}

    ),
    date_details as (select * from {{ ref("date_details") }}),
    department_info as (

        select * from {{ ref("bamboohr_job_info_current_division_base") }}

    ),
    job_role as (select * from {{ ref("bamboohr_job_role") }}),
    location_factor as (select * from {{ ref("employee_location_factor_snapshots") }}),
    employment_status as (select * from {{ ref("bamboohr_employment_status_xf") }}),
    promotion as (

        select employee_id, effective_date, compensation_change_reason
        from {{ ref("bamboohr_compensation_source") }}
        where compensation_change_reason = 'Promotion'
        group by 1, 2, 3

    ),
    direct_reports as (

        select
            date_actual as date, reports_to, count(employee_id) as total_direct_reports
        from
            (
                select
                    date_details.date_actual, employee_directory.employee_id, reports_to
                from date_details
                left join
                    employee_directory
                    on employee_directory.hire_date::date <= date_actual
                    and coalesce(
                        termination_date::date, {{ max_date_in_bamboo_analyses() }}
                    )
                    >= date_actual
                left join
                    department_info
                    on employee_directory.employee_id = department_info.employee_id
                    and date_details.date_actual
                    between department_info.effective_date and coalesce(
                        department_info.effective_end_date,
                        {{ max_date_in_bamboo_analyses() }}
                    )
            )
        group by 1, 2
        having total_direct_reports > 0

    ),
    job_info_mapping_historical as (

        select
            department_info.employee_id,
            department_info.job_title,
            iff(
                job_title = 'Manager, Field Marketing',
                'Leader',
                coalesce(job_role.job_role, department_info.job_role)
            ) as job_role,
            case
                when job_title = 'Group Manager, Product'
                then '9.5'
                when job_title = 'Manager, Field Marketing'
                then '8'
                else job_role.job_grade
            end as job_grade,
            row_number() over (
                partition by department_info.employee_id
                order by date_details.date_actual
            ) as job_grade_event_rank
        from date_details
        left join
            department_info
            on date_details.date_actual
            between department_info.effective_date and coalesce(
                department_info.effective_end_date, {{ max_date_in_bamboo_analyses() }}
            )
        left join
            job_role
            on job_role.employee_id = department_info.employee_id
            and date_details.date_actual between job_role.effective_date and coalesce(
                job_role.next_effective_date, {{ max_date_in_bamboo_analyses() }}
            )
        where job_role.job_grade is not null
    -- -Using the 1st time we captured job_role and grade to identify classification
    -- for historical records
    ),
    employment_status_records_check as (

        select employee_id, min(valid_from_date) as employment_status_first_value
        from {{ ref("bamboohr_employment_status_xf") }}
        group by 1

    ),
    cost_center_prior_to_bamboo as (

        select * from {{ ref("cost_center_division_department_mapping") }}

    ),
    sheetload_engineering_speciality as (

        select * from {{ ref("sheetload_engineering_speciality_prior_to_capture") }}

    ),
    bamboohr_discretionary_bonuses_xf as (

        select * from {{ ref("bamboohr_directionary_bonuses_xf") }}

    ),
    fct_work_email as (select * from {{ ref("bamboohr_work_email") }}),
    enriched as (

        select
            date_details.date_actual,
            employee_directory.*,
            coalesce(
                fct_work_email.work_email, employee_directory.last_work_email
            ) as work_email,
            department_info.job_title,
            department_info.department,
            department_info.department_modified,
            department_info.department_grouping,
            department_info.division,
            department_info.division_mapped_current,
            department_info.division_grouping,
            coalesce(
                job_role.cost_center, cost_center_prior_to_bamboo.cost_center
            ) as cost_center,
            department_info.reports_to,
            iff(
                date_details.date_actual between '2019-11-01' and '2020-02-27'
                and job_info_mapping_historical.job_role is not null,
                job_info_mapping_historical.job_role,
                coalesce(job_role.job_role, department_info.job_role)
            ) as job_role,
            iff(
                date_details.date_actual between '2019-11-01' and '2020-02-27',
                job_info_mapping_historical.job_grade,
                job_role.job_grade
            ) as job_grade,
            coalesce(
                sheetload_engineering_speciality.speciality,
                job_role.jobtitle_speciality
            ) as jobtitle_speciality,
            -- -to capture speciality for engineering prior to 2020.09.30 we are using
            -- sheetload, and capturing from bamboohr afterwards
            location_factor.location_factor as location_factor,
            location_factor.locality,
            iff(
                employee_directory.hire_date = date_actual or rehire_date = date_actual,
                true,
                false
            ) as is_hire_date,
            iff(employment_status = 'Terminated', true, false) as is_termination_date,
            iff(rehire_date = date_actual, true, false) as is_rehire_date,
            iff(
                employee_directory.hire_date < employment_status_first_value,
                'Active',
                employment_status
            ) as employment_status,
            job_role.gitlab_username,
            sales_geo_differential,
            direct_reports.total_direct_reports,
            -- for the diversity KPIs we are looking to understand senior leadership
            -- representation and do so by job grade instead of role
            case
                when
                    (
                        left(department_info.job_title, 5) = 'Staff'
                        or left(department_info.job_title, 13) = 'Distinguished'
                        or left(department_info.job_title, 9) = 'Principal'
                    )
                    and coalesce(
                        job_role.job_grade, job_info_mapping_historical.job_grade
                    )
                    in ('8', '9', '9.5', '10')
                then 'Staff'
                when department_info.job_title ilike '%Fellow%'
                then 'Staff'
                when
                    coalesce(job_role.job_grade, job_info_mapping_historical.job_grade)
                    in ('11', '12', '14', '15', 'CXO')
                then 'Senior Leadership'
                when
                    coalesce(job_role.job_grade, job_info_mapping_historical.job_grade)
                    like '%C%'
                then 'Senior Leadership'
                when
                    (
                        department_info.job_title like '%VP%'
                        or department_info.job_title like '%Chief%'
                        or department_info.job_title like '%Senior Director%'
                    )
                    and coalesce(
                        job_role.job_role,
                        job_info_mapping_historical.job_role,
                        department_info.job_role
                    )
                    = 'Leader'
                then 'Senior Leadership'
                when
                    coalesce(job_role.job_grade, job_info_mapping_historical.job_grade)
                    = '10'
                then 'Manager'
                when
                    coalesce(
                        job_role.job_role,
                        job_info_mapping_historical.job_role,
                        department_info.job_role
                    )
                    = 'Manager'
                then 'Manager'
                when coalesce(total_direct_reports, 0) = 0
                then 'Individual Contributor'
                else
                    coalesce(
                        job_role.job_role,
                        job_info_mapping_historical.job_role,
                        department_info.job_role
                    )
            end as job_role_modified,
            iff(compensation_change_reason is not null, true, false) as is_promotion,
            bamboohr_discretionary_bonuses_xf.total_discretionary_bonuses
            as discretionary_bonus,
            row_number() over (
                partition by employee_directory.employee_id order by date_actual
            ) as tenure_days
        from date_details
        left join
            employee_directory
            on employee_directory.hire_date::date <= date_actual
            and coalesce(termination_date::date, {{ max_date_in_bamboo_analyses() }})
            >= date_actual
        left join
            department_info
            on employee_directory.employee_id = department_info.employee_id
            and date_actual between effective_date and coalesce(
                effective_end_date::date, {{ max_date_in_bamboo_analyses() }}
            )
        left join
            direct_reports
            on direct_reports.date = date_details.date_actual
            and direct_reports.reports_to = employee_directory.full_name
        left join
            location_factor
            on employee_directory.employee_number::varchar
            = location_factor.bamboo_employee_number::varchar
            and valid_from <= date_actual
            and coalesce(valid_to::date, {{ max_date_in_bamboo_analyses() }})
            >= date_actual
        left join
            employment_status
            on employee_directory.employee_id = employment_status.employee_id
            and (
                date_details.date_actual = valid_from_date
                and employment_status = 'Terminated'
                or date_details.date_actual
                between employment_status.valid_from_date
                and employment_status.valid_to_date
            )
        left join
            employment_status_records_check
            on employee_directory.employee_id
            = employment_status_records_check.employee_id
        left join
            cost_center_prior_to_bamboo
            on department_info.department = cost_center_prior_to_bamboo.department
            and department_info.division = cost_center_prior_to_bamboo.division
            and date_details.date_actual
            between cost_center_prior_to_bamboo.effective_start_date and coalesce(
                cost_center_prior_to_bamboo.effective_end_date, '2020-05-07'
            )
        -- -Starting 2020.05.08 we start capturing cost_center in bamboohr
        left join
            job_role
            on employee_directory.employee_id = job_role.employee_id
            and date_details.date_actual between job_role.effective_date and coalesce(
                job_role.next_effective_date, {{ max_date_in_bamboo_analyses() }}
            )
        left join
            job_info_mapping_historical
            on employee_directory.employee_id = job_info_mapping_historical.employee_id
            and job_info_mapping_historical.job_title = department_info.job_title
            and job_info_mapping_historical.job_grade_event_rank = 1
        -- -tying data based on 2020-02-27 date to historical data --
        left join
            promotion
            on promotion.employee_id = employee_directory.employee_id
            and date_details.date_actual = promotion.effective_date
        left join
            sheetload_engineering_speciality
            on employee_directory.employee_id
            = sheetload_engineering_speciality.employee_id
            and date_details.date_actual
            between sheetload_engineering_speciality.speciality_start_date and coalesce(
                sheetload_engineering_speciality.speciality_end_date, '2020-09-30'
            )
        -- -Post 2020.09.30 we will capture engineering speciality from bamboohr
        left join
            bamboohr_discretionary_bonuses_xf
            on employee_directory.employee_id
            = bamboohr_discretionary_bonuses_xf.employee_id
            and date_details.date_actual = bamboohr_discretionary_bonuses_xf.bonus_date
        left join
            fct_work_email
            on employee_directory.employee_id = fct_work_email.employee_id
            and date_details.date_actual
            between fct_work_email.valid_from_date and fct_work_email.valid_to_date
        where employee_directory.employee_id is not null

    ),
    base_layers as (

        select
            date_actual,
            reports_to,
            full_name,
            array_construct(reports_to, full_name) as lineage
        from enriched
        where nullif(reports_to, '') is not null

    ),
    layers(date_actual, employee, manager, lineage, layers_count) as (

        select
            date_actual,
            full_name as employee,
            reports_to as manager,
            lineage as lineage,
            1 as layers_count
        from base_layers
        where manager is not null

        union all

        select
            anchor.date_actual,
            iter.full_name as employee,
            iter.reports_to as manager,
            array_prepend(anchor.lineage, iter.reports_to) as lineage,
            (layers_count + 1) as layers_count
        from layers anchor
        join
            base_layers iter
            on anchor.date_actual = iter.date_actual
            and iter.reports_to = anchor.employee

    ),
    calculated_layers as (

        select date_actual, employee, max(layers_count) as layers
        from layers
        group by 1, 2

    )

select enriched.*, coalesce(calculated_layers.layers, 1) as layers
from enriched
left join
    calculated_layers
    on enriched.date_actual = calculated_layers.date_actual
    and full_name = employee
    and enriched.employment_status is not null
where employment_status is not null

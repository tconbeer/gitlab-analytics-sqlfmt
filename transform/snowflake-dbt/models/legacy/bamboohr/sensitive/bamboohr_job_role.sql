with
    source as (select * from {{ ref("bamboohr_id_employee_number_mapping_source") }}),
    intermediate as (

        select
            employee_number,
            employee_id,
            first_name,
            last_name,
            hire_date,
            termination_date,
            job_role,
            job_grade,
            cost_center,
            case
                when
                    jobtitle_speciality_multi_select is null
                    and jobtitle_speciality_single_select is null
                then null
                when jobtitle_speciality_single_select is null
                then jobtitle_speciality_multi_select
                when jobtitle_speciality_multi_select is null
                then jobtitle_speciality_single_select
                else
                    jobtitle_speciality_single_select
                    || ','
                    || jobtitle_speciality_multi_select
            end as jobtitle_speciality,
            gitlab_username,
            pay_frequency,
            sales_geo_differential,
            date_trunc(day, uploaded_at) as effective_date,
            {{
                dbt_utils.surrogate_key(
                    [
                        "employee_id",
                        "job_role",
                        "job_grade",
                        "cost_center",
                        "jobtitle_speciality",
                        "gitlab_username",
                        "pay_frequency",
                        "sales_geo_differential",
                    ]
                )
            }} as unique_key
        from source
        qualify
            row_number() over (
                partition by unique_key
                order by
                    date_trunc(day, effective_date) asc,
                    date_trunc(hour, effective_date) desc
            )
            = 1

    ),
    final as (

        select
            *,
            lead(dateadd(day, -1, date_trunc(day, intermediate.effective_date))) over (
                partition by employee_number order by intermediate.effective_date
            ) as next_effective_date
        from intermediate
        where
            effective_date
            >= '2020-02-27'  -- 1st day we started capturing job role

    )

select *
from final

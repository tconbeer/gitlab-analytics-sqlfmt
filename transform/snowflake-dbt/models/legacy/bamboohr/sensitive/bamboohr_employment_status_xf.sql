with
    bamboohr_employment_status as (

        select * from {{ ref("bamboohr_employment_status_source") }}

    ),
    employment_log as (

        select
            status_id,
            employee_id,
            employment_status,
            termination_type,
            effective_date as valid_from_date,
            lead(effective_date) over (
                partition by employee_id order by effective_date, status_id
            ) as valid_to_date,
            lead(employment_status) over (
                partition by employee_id order by effective_date, status_id
            ) as next_employment_status,
            lag(employment_status) over (
                partition by employee_id order by effective_date, status_id
            ) as previous_employment_status
        from bamboohr_employment_status

    ),
    final as (

        select
            employee_id,
            employment_status,
            termination_type,
            case
                when
                    previous_employment_status = 'Terminated'
                    and employment_status != 'Terminated'
                then 'True'
                else 'False'
            end as is_rehire,
            next_employment_status,
            valid_from_date as valid_from_date,
            iff(
                employment_status = 'Terminated',
                valid_from_date,
                coalesce(
                    dateadd('day', -1, valid_to_date),
                    {{ max_date_in_bamboo_analyses() }}
                )
            ) as valid_to_date
        from employment_log
    )

select *
from final

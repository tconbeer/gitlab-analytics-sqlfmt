with
    source as (select * from {{ ref("employee_directory_intermediate") }}),
    intermediate as (
        select distinct
            cost_center, division, department, count(employee_id) as total_employees
        from source
        where date_actual = current_date() and is_termination_date = false
        group by 1, 2, 3

    )

select *
from intermediate
qualify row_number() over (partition by department order by total_employees desc) = 1

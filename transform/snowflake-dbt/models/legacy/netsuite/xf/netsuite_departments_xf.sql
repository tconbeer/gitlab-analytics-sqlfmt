with
    base_departments as (select * from {{ ref("netsuite_departments_source") }}),
    parent_department as (

        select
            a.department_id,
            a.department_name,
            a.department_full_name,
            a.is_department_inactive,
            case
                when a.parent_department_id is not null
                then a.parent_department_id
                else a.department_id
            end as parent_department_id,
            case
                when a.parent_department_id is not null
                then b.department_name
                else a.department_name
            end as parent_department_name
        from base_departments a
        left join base_departments b on a.parent_department_id = b.department_id

    )

select *
from parent_department

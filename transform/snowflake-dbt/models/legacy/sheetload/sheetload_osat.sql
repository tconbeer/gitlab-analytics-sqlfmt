with
    bamboohr as (select * from {{ ref("employee_directory") }}),
    source as (select * from {{ ref("sheetload_osat_source") }}),
    final as (

        select
            source.completed_date,
            coalesce(
                date_trunc(month, source.hire_date),
                date_trunc(month, bamboohr.hire_date)
            ) as hire_month,
            source.division,
            source.satisfaction_score,
            source.recommend_to_friend,
            source.buddy_experience_score
        from source
        left join
            bamboohr on source.employee_name = concat(
                bamboohr.first_name, ' ', bamboohr.last_name
            )
        where source.completed_date is not null

    )
select *
from final

with
    source as (

        select * from {{ source("sheetload", "infrastructure_missing_employees") }}

    ),
    final as (

        select
            nullif(employee_id, '')::integer as employee_id,
            nullif(gitlab_dotcom_user_id, '')::varchar as gitlab_dotcom_user_id,
            nullif(full_name, '')::varchar as full_name,
            nullif(work_email, '')::varchar as work_email
        from source

    )

select *
from final

with
    source as (select * from {{ source("greenhouse", "jobs_departments") }}),
    renamed as (

        select

            -- keys
            id::number as job_department_id,
            job_id::number as job_id,
            department_id::number as department_id,

            -- info
            created_at::timestamp as job_department_created_at,
            updated_at::timestamp as job_department_updated_at

        from source

    )

select *
from renamed

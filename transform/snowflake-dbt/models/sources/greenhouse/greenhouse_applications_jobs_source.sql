with
    source as (select * from {{ source("greenhouse", "applications_jobs") }}),
    renamed as (

        select
            -- keys
            application_id::number as application_id, job_id::number as job_id

        from source

    )

select *
from renamed

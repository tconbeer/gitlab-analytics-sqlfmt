with
    source as (select * from {{ source("greenhouse", "jobs_offices") }}),
    renamed as (

        select

            -- keys
            id::number as job_office_id,
            job_id::number as job_id,
            office_id::number as office_id,

            -- info
            created_at::timestamp as job_office_created_at,
            updated_at::timestamp as job_office_updated_at

        from source

    )

select *
from renamed

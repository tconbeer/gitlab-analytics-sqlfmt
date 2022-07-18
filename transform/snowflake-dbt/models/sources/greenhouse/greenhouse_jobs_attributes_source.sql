with
    source as (select * from {{ source("greenhouse", "jobs_attributes") }}),
    renamed as (

        select

            -- keys
            id::number as job_attribute_id,
            job_id::number as job_id,
            attribute_id::number as attribute_id,

            -- info
            active::boolean as is_active,
            created_at::timestamp as jobs_attribute_created_at,
            updated_at::timestamp as jobs_attribute_updated_at

        from source

    )

select *
from renamed

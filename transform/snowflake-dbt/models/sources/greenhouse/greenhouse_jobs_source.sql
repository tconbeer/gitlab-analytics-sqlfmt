with
    source as (select * from {{ source("greenhouse", "jobs") }}),
    renamed as (

        select

            -- keys
            id::number as job_id,
            organization_id::number as organization_id,
            requisition_id::varchar as requisition_id,
            department_id::number as department_id,

            -- info
            name::varchar as job_name,
            status::varchar as job_status,
            opened_at::timestamp as job_opened_at,
            closed_at::timestamp as job_closed_at,
            level::varchar as job_level,
            confidential::boolean as is_confidential,
            created_at::timestamp as job_created_at,
            notes::varchar as job_notes,
            updated_at::timestamp as job_updated_at

        from source

    )

select *
from renamed

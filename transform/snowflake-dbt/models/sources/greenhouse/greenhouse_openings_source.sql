with
    source as (select * from {{ source("greenhouse", "openings") }}),
    renamed as (

        select
            -- keys
            id::number as job_opening_id,
            job_id::number as job_id,
            opening_id::varchar as opening_id,
            hired_application_id::number as hired_application_id,

            -- info
            opened_at::timestamp as job_opened_at,
            closed_at::timestamp as job_closed_at,
            close_reason::varchar as close_reason,
            created_at::timestamp as job_opening_created_at,
            updated_at::timestamp as job_opening_updated_at,
            target_start_date::date as target_start_date

        from source

    )

select *
from renamed

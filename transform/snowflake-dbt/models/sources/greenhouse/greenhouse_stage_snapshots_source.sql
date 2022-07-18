with
    source as (select * from {{ source("greenhouse", "stage_snapshots") }}),
    renamed as (

        select
            -- keys
            stage_id::number as stage_snapshot_id,
            job_id::number as job_id,

            -- info
            date::date as stage_snapshot_date,
            active_count::number as stage_snapshot_active_count
        from source

    )

select *
from renamed

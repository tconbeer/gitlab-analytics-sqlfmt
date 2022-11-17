with
    source as (select * from {{ source("greenhouse", "job_snapshots") }}),
    renamed as (

        select

            -- key
            job_id::number as job_id,

            -- info
            date::date as job_snapshot_date,
            hired_count::number as hired_count,
            prospect_count::number as prospect_count,
            new_today::number as new_today,
            rejected_today::number as rejected_today,
            advanced_today::number as advanced_today,
            interviews_today::number as interviews_today

        from source

    )

select *
from renamed

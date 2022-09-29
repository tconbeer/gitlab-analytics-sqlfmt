with
    source as (

        select * from {{ ref("gitlab_dotcom_project_mirror_data_dedupe_source") }}


    ),
    renamed as (

        select

            id::number as project_mirror_data_id,
            project_id::number as project_id,
            retry_count::number as retry_count,
            last_update_started_at::timestamp as last_update_started_at,
            last_update_scheduled_at::timestamp as last_update_scheduled_at,
            next_execution_timestamp::timestamp as next_execution_timestamp

        from source

    )

select *
from renamed

with
    source as (

        select *
        from {{ source("gitlab_ops", "merge_request_metrics") }}
        qualify row_number() over (partition by id order by updated_at desc) = 1

    ),
    renamed as (

        select
            id::number as merge_request_metric_id,
            merge_request_id::number as merge_request_id,

            latest_build_started_at::timestamp as latest_build_started_at,
            latest_build_finished_at::timestamp as latest_build_finished_at,
            first_deployed_to_production_at::timestamp
            as first_deployed_to_production_at,
            merged_at::timestamp as merged_at,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at

        from source

    )

select *
from renamed

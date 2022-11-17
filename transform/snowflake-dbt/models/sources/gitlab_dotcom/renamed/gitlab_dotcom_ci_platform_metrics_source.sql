with
    source as (

        select * from {{ ref("gitlab_dotcom_ci_platform_metrics_dedupe_source") }}

    ),
    renamed as (

        select
            id::number as metric_id,
            recorded_at::timestamp as recorded_at,
            platform_target::varchar as platform_target,
            count::number as target_count
        from source

    )

select *
from renamed

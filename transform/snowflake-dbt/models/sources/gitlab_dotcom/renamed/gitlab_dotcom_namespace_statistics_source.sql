with
    source as (

        select * from {{ ref("gitlab_dotcom_namespace_statistics_dedupe_source") }}

    ),
    renamed as (

        select

            id::number as namespace_statistics_id,
            namespace_id::number as namespace_id,
            shared_runners_seconds::number as shared_runners_seconds,
            shared_runners_seconds_last_reset::timestamp
            as shared_runners_seconds_last_reset

        from source

    )

select *
from renamed

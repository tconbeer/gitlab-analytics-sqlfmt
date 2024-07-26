with
    source as (

        select * from {{ ref("gitlab_dotcom_resource_weight_events_dedupe_source") }}

    ),
    renamed as (

        select
            id as resource_weight_event_id,
            user_id::number as user_id,
            issue_id::number as issue_id,
            weight::number as weight,
            created_at::timestamp as created_at
        from source

    )

select *
from renamed

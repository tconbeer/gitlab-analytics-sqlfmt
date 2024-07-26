with
    source as (

        select * from {{ ref("gitlab_dotcom_resource_milestone_events_dedupe_source") }}

    ),
    renamed as (

        select
            id as resource_milestone_event_id,
            action::number as action_type_id,
            {{ resource_event_action_type("action") }} as action_type,
            user_id::number as user_id,
            issue_id::number as issue_id,
            merge_request_id::number as merge_request_id,
            milestone_id::number as milestone_id,
            {{ map_state_id("state") }} as milestone_state,
            created_at::timestamp as created_at
        from source

    )

select *
from renamed

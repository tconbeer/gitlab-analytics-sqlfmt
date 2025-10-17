with
    source as (

        select * from {{ ref("gitlab_dotcom_resource_label_events_dedupe_source") }}

    ),
    renamed as (

        select
            id as resource_label_event_id,
            action::number as action_type_id,
            {{ resource_event_action_type("action") }} as action_type,
            issue_id::number as issue_id,
            merge_request_id::number as merge_request_id,
            epic_id::number as epic_id,
            label_id::number as label_id,
            user_id::number as user_id,
            created_at::timestamp as created_at,
            cached_markdown_version::varchar as cached_markdown_version,
            reference::varchar as referrence,
            reference_html::varchar as reference_html
        from source

    )

select *
from renamed
